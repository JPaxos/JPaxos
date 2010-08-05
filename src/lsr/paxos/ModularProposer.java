package lsr.paxos;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import lsr.common.NoOperationRequest;
import lsr.common.Request;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * Represents part of paxos which is responsible for proposing new consensus
 * values. Provides procedures to start proposing which sends the
 * {@link Propose} messages, and allows proposing new values. The number of
 * currently running proposals is defined by <code>MAX_ACTIVE_PROPOSALS</code>.
 */
class ModularProposer implements Proposer {

	private final ArrayDeque<Request> _pendingProposals = new ArrayDeque<Request>();
	/** retransmitted message for prepare request */
	private RetransmittedMessage _prepareRetransmitter;

	/** retransmitted propose messages for instances */
	private Map<Integer, RetransmittedMessage> _proposeRetransmitters = new HashMap<Integer, RetransmittedMessage>();

	/** Keeps track of the processes that have prepared for this view */
	private BitSet _prepared = new BitSet();
	private final Retransmitter _retransmitter;
	private final Paxos _paxos;
	private final Storage _storage;
	private final Network _network;
	private ProposerState _state;

	/**
	 * Initializes new instance of <code>Proposer</code>. If the id of current
	 * replica is 0 then state is set to <code>ACTIVE</code>. Otherwise
	 * <code>INACTIVE</code> state is set.
	 * 
	 * @param paxos
	 *            - the paxos the acceptor belong to
	 * @param network
	 *            - data associated with the paxos
	 * @param failureDetector
	 *            - used to notify about leader change
	 * @param storage
	 *            - used to send responses
	 */
	public ModularProposer(Paxos paxos, Network network, Storage storage) {
		_paxos = paxos;
		_network = network;
		_storage = storage;
		_retransmitter = new Retransmitter(_network, _storage.getN());

		// if (_storage.getLocalId() == 0)
		// _state = ProposerState.PREPARED;
		// else
		_state = ProposerState.INACTIVE;
	}

	/**
	 * Gets the current state of the proposer.
	 * 
	 * @return <code>ACTIVE</code> if the current proposer can propose new
	 *         values, <code>INACTIVE</code> otherwise
	 */
	public ProposerState getState() {
		return _state;
	}

	/**
	 * If previous leader is suspected this procedure is executed. We're
	 * changing the view (variable indicating order of the leaders in time)
	 * accordingly, and we're sending the prepare message.
	 * 
	 */
	public void prepareNextView() {
		assert _state == ProposerState.INACTIVE : "Proposer is active.";
		assert _paxos.getDispatcher().amIInDispatcher();

		_prepared.clear();
		_state = ProposerState.PREPARING;
		setNextViewNumber();

		Prepare prepare = new Prepare(_storage.getStableStorage().getView(), _storage.getFirstUncommitted());
		_prepareRetransmitter = _retransmitter.startTransmitting(prepare, _storage.getAcceptors());

		_logger.fine("Advancing to view " + _storage.getStableStorage().getView());
	}

	private void setNextViewNumber() {
		int view = _storage.getStableStorage().getView();
		do {
			view++;
		} while (view % _storage.getN() != _storage.getLocalId());
		_storage.getStableStorage().setView(view);
	}

	/**
	 * 
	 * @param message
	 * @param sender
	 * @throws InterruptedException
	 */
	public void onPrepareOK(PrepareOK message, int sender) {
		assert _paxos.getDispatcher().amIInDispatcher();
		assert _paxos.isLeader();
		assert _state != ProposerState.INACTIVE : "Proposer is not active.";
		// A process sends a PrepareOK message only as a response to a
		// Prepare message. Therefore, for this process to receive such
		// a message it must have sent a prepare message, so it must be
		// on a phase equal or higher than the phase of the prepareOk
		// message.
		assert message.getView() == _storage.getStableStorage().getView() : "Received a PrepareOK for a higher or lower view. "
				+ "Msg.view: " + message.getView() + ", view: " + _storage.getStableStorage().getView();

		// Ignore prepareOK messages if we have finished preparing
		if (_state == ProposerState.PREPARED) {
			_logger.fine("View " + _storage.getStableStorage().getView() + " already prepared. Ignoring message.");
			return;
		}

		updateLogFromPrepareOk(message);

		_prepared.set(sender);
		_prepareRetransmitter.stop(sender);

		if (isMajority())
			stopPreparingStartProposing();
	}

	private void stopPreparingStartProposing() {
		_prepareRetransmitter.stop();
		_prepareRetransmitter = null;
		_state = ProposerState.PREPARED;

		_logger.info("Finished preparing view " + _storage.getStableStorage().getView());

		// Send a proposal for all instances that were not decided.
		Log log = _storage.getLog();
		for (int i = _storage.getFirstUncommitted(); i < log.getNextId(); i++) {
			ConsensusInstance instance = log.getInstance(i);
			// May happen if prepareOK caused a snapshot
			if (instance == null)
				continue;
			switch (instance.getState()) {
				case DECIDED:
					// If the decision was already taken by some process,
					// there is no need to propose again, so skip this
					// instance
					break;

				case KNOWN:
					// No decision, but some process already accepted it.
					_logger.info("Proposing locked value: " + instance);
					instance.setView(_storage.getStableStorage().getView());
					continueProposal(instance);
					break;

				case UNKNOWN:
					assert instance.getValue() == null : "Unknow instance has value";
					_logger.info("No value locked for instance " + i + ": proposing no-op");
					fillWithNoOperation(instance);
			}
		}
		sendNextProposal();
	}

	private void fillWithNoOperation(ConsensusInstance instance) {
		instance.setValue(_storage.getStableStorage().getView(), new NoOperationRequest().toByteArray());
		continueProposal(instance);
	}

	private boolean isMajority() {
		return _prepared.cardinality() > _storage.getN() / 2;
	}

	private void updateLogFromPrepareOk(PrepareOK message) {
		if (message.getPrepared() == null)
			return;
		// Update the local log with the data sent by this process
		for (int i = 0; i < message.getPrepared().length; i++) {
			ConsensusInstance ci = message.getPrepared()[i];
			// Algorithm: The received instance can be either
			// Decided - Set the local log entry to decided.
			// Accepted - If the local log entry is decided, ignore.
			// Otherwise, find the accept message for this consensus
			// instance with the highest timestamp and propose it.
			ConsensusInstance localLog = _storage.getLog().getInstance(ci.getId());
			// Happens if previous PrepareOK caused a snapshot execution
			if (localLog == null)
				continue;
			if (localLog.getState() == LogEntryState.DECIDED) {
				// We already know the decision, so ignore it.
				continue;
			}
			switch (ci.getState()) {
				case DECIDED:
					localLog.setValue(ci.getView(), ci.getValue());
					_paxos.decide(ci.getId());
					break;

				case KNOWN:
					localLog.setValue(ci.getView(), ci.getValue());
					break;

				case UNKNOWN:
					assert ci.getValue() == null : "Unknow instance has value";
					_logger.fine("Ignoring: " + ci);
					break;

				default:
					assert false : "Invalid state: " + ci.getState();
					break;
			}
		}

	}

	/**
	 * Asks the proposer to propose the given value. If there are currently too
	 * many active propositions, this proposal will be enqueued until there are
	 * available slots. If the proposer is <code>INACTIVE</code>, then message
	 * is discarded. Otherwise value is added to list of active proposals.
	 * 
	 * @param value
	 *            - the value to propose
	 */
	public void propose(Request value) {
		assert _paxos.getDispatcher().amIInDispatcher();

		if (_state == ProposerState.INACTIVE)
			return;

		if (_pendingProposals.contains(value)) {
			_logger.info("Ignoring propose" + value);
			return;
		}

		_pendingProposals.add(value);
		sendNextProposal();
	}

	/**
	 * Called to inform the proposer that a decision was taken. Allows the
	 * proposer to make a new proposal.
	 */
	public void ballotFinished() {
		assert _paxos.getDispatcher().amIInDispatcher();

		sendNextProposal();
	}

	/**
	 * As leader, activated when a proposal from client reaches the machine, or
	 * a decision was taken.
	 * 
	 * Checks if the proposal is still inside the window (that is: if there are
	 * not too many concurrent instances)
	 * 
	 */
	private void sendNextProposal() {
		if (!canSendNextProposal())
			return;

		try {
			assert _state == ProposerState.PREPARED;
			assert _prepareRetransmitter == null : "Prepare round unfinished and a proposal issued";

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);

			// later filled with count of instances
			dos.writeInt(0);

			int count = 1;
			Request request;
			byte[] requestByteArray;

			request = _pendingProposals.poll();
			requestByteArray = request.toByteArray();
			dos.writeInt(requestByteArray.length);
			dos.write(requestByteArray);
			
			StringBuilder sb = new StringBuilder();
			sb.append(request.getRequestId().toString());
			
			while (!_pendingProposals.isEmpty()) {
				request = _pendingProposals.getFirst();
				requestByteArray = request.toByteArray();
				/* TODO: 32 == approximate 'rest of the message' size */
//				if ((baos.size() + requestByteArray.length) > Config.BATCHING_LEVEL) {
				if ((baos.size() + requestByteArray.length) > _paxos.getProcessDescriptor().batchingLevel) {
					break;
				}
				dos.writeInt(requestByteArray.length);
				dos.write(requestByteArray);
				_pendingProposals.remove(request);
				count++;
				sb.append(", ").append(request.getRequestId().toString());
			}

			byte[] value = baos.toByteArray();
			ByteBuffer.wrap(value).putInt(count);

			ConsensusInstance instance = _storage.getLog().append(_storage.getStableStorage().getView(), value);

			assert _proposeRetransmitters.containsKey(instance.getId()) == false : "Different proposal for the same instance";

			// creating retransmitter, which automatically starts
			// sending propose message to all acceptors
			Message message = new Propose(instance);
			_proposeRetransmitters.put(instance.getId(), _retransmitter.startTransmitting(message,
					_storage.getAcceptors()));

			_logger.info("Proposing: " + instance + ", ids: " + sb);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean canSendNextProposal() {
		if (_state == ProposerState.PREPARING)
			return false;
		if (!_storage.isInWindow(_storage.getLog().getNextId()))
			return false;
		if (_pendingProposals.isEmpty())
			return false;
		return true;
	}

	/**
	 * After becoming the leader we need to take control over the consensus for
	 * orphaned instances. This method activates retransmission of propose
	 * messages for instances, which we already have in our logs (
	 * {@link sendNextProposal} and {@link Propose} create a new instance)
	 * 
	 * @param instance
	 *            instance we want to revoke
	 */
	private void continueProposal(ConsensusInstance instance) {
		assert _state == ProposerState.PREPARED;
		assert _prepareRetransmitter == null : "Prepare round unfinished and a proposal issued";
		assert _proposeRetransmitters.containsKey(instance.getId()) == false : "Different proposal for the same instance";

		// TODO: current implementation causes temporary window size violation.
		Message m = new Propose(instance);
		_proposeRetransmitters.put(instance.getId(), _retransmitter.startTransmitting(m, _storage.getAcceptors()));
	}

	/**
	 * As the process looses leadership, it must stop all message retransmission
	 * - that is either prepare or propose messages.
	 */
	public void stopProposer() {
		_state = ProposerState.INACTIVE;
		_pendingProposals.clear();

		if (_prepareRetransmitter != null) {
			_prepareRetransmitter.stop();
			_prepareRetransmitter = null;
		} else {
			_retransmitter.stopAll();
			_proposeRetransmitters.clear();
		}
	}

	/**
	 * After reception of majority accepts, we suppress propose messages.
	 * 
	 * @param instanceId
	 *            no. of instance, for which we want to stop retransmission
	 */
	public void stopPropose(int instanceId) {
		assert _paxos.getDispatcher().amIInDispatcher();

		RetransmittedMessage r = _proposeRetransmitters.remove(instanceId);
		if (r != null)
			r.stop();
	}

	/**
	 * If retransmission to some process for certain instance is no longer
	 * needed, we should stop it
	 * 
	 * @param instanceId
	 *            no. of instance, for which we want to stop retransmission
	 * @param destination
	 *            number of the process in processes PID list
	 */
	public void stopPropose(int instanceId, int destination) {
		assert _proposeRetransmitters.containsKey(instanceId);
		assert _paxos.getDispatcher().amIInDispatcher();

		_proposeRetransmitters.get(instanceId).stop(destination);
	}

	private final static Logger _logger = Logger.getLogger(ModularProposer.class.getCanonicalName());
}
