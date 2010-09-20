package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher.Priority;
import lsr.common.NoOperationRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.statistics.ReplicaStats;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

/**
 * Represents part of paxos which is responsible for proposing new consensus
 * values. Provides procedures to start proposing which sends the
 * {@link Propose} messages, and allows proposing new values. The number of
 * currently running proposals is defined by <code>MAX_ACTIVE_PROPOSALS</code>.
 */
class ProposerImpl implements Proposer {

	/** retransmitted message for prepare request */
	private RetransmittedMessage _prepareRetransmitter;

	/** retransmitted propose messages for instances */
	private final Map<Integer, RetransmittedMessage> _proposeRetransmitters = new HashMap<Integer, RetransmittedMessage>();

	/** Keeps track of the processes that have prepared for this view */
	private BitSet _prepared = new BitSet();
	private final Retransmitter _retransmitter;
	private final Paxos _paxos;
	private final Storage _storage;
	private final StableStorage _stableStorage;
	private final FailureDetector _failureDetector;
	private final Network _network;


	private final ArrayDeque<Request> _pendingProposals = 
		new ArrayDeque<Request>();
	private ProposerState _state;
//	private Batcher _batcher;

	private BatchBuilder batchBuilder;

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
	public ProposerImpl(Paxos paxos, Network network,
			FailureDetector failureDetector, Storage storage) {
		_paxos = paxos;
		_network = network;
		_failureDetector = failureDetector;
		_storage = storage;
		_retransmitter = new Retransmitter(_network, _storage.getN(), _paxos
		                                   .getDispatcher());
		_stableStorage = storage.getStableStorage();

//		_batcher = new BatcherImpl(ProcessDescriptor.getInstance().batchingLevel);
		
		// Start view 0. Process 0 assumes leadership
		// without executing a prepare round, since there's
		// nothing to prepare
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
		assert _state == ProposerState.INACTIVE : "Proposer is ACTIVE.";
		assert _paxos.getDispatcher().amIInDispatcher();

		_prepared.clear();
		_state = ProposerState.PREPARING;
		setNextViewNumber();
		_failureDetector.leaderChange(_paxos.getLeaderId());

		Prepare prepare = new Prepare(_stableStorage.getView(), _storage
		                              .getFirstUncommitted());
		_prepareRetransmitter = _retransmitter.startTransmitting(prepare,
		                                                         _storage.getAcceptors());

		_logger.info("Preparing view: " + _stableStorage.getView());
	}

	private void setNextViewNumber() {
		int view = _stableStorage.getView();
		do {
			view++;
		} while (view % _storage.getN() != _storage.getLocalId());
		_stableStorage.setView(view);
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
		assert message.getView() == _stableStorage.getView() : "Received a PrepareOK for a higher or lower view. "
			+ "Msg.view: "
			+ message.getView()
			+ ", view: "
			+ _stableStorage.getView();

		// Ignore prepareOK messages if we have finished preparing
		if (_state == ProposerState.PREPARED) {
			if (_logger.isLoggable(Level.FINE)) {
				_logger.fine("View " + _stableStorage.getView()
				             + " already prepared. Ignoring message.");
			}
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

		_logger.info("View prepared " + _stableStorage.getView());
		ReplicaStats.getInstance().advanceView(_stableStorage.getView());

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
				instance.setView(_stableStorage.getView());
				continueProposal(instance);
				break;

			case UNKNOWN:
				assert instance.getValue() == null : "Unknow instance has value";
				_logger.info("No value locked for instance " + i
				             + ": proposing no-op");
				fillWithNoOperation(instance);
			}
		}
		
		batchBuilder = new BatchBuilder();
		// TODO: NS: Probably not needed as there is no proposal waiting
		// Shouldn't the leader send propose for unfinished instances?
		batchBuilder.enqueueRequests();
//		sendNextProposal();
	}

	private void fillWithNoOperation(ConsensusInstance instance) {
		instance.setValue(_stableStorage.getView(), new NoOperationRequest()
		.toByteArray());
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
			ConsensusInstance localLog = 
				_storage.getLog().getInstance(ci.getId());
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

		if (_state == ProposerState.INACTIVE) {
			_logger.warning("Cannot propose on inactive state: " + value);
			return;
		}

		if (_pendingProposals.contains(value)) {
			_logger.warning("Value already queued for proposing. Ignoring: "
			                + value);
			return;
		}

		_pendingProposals.add(value);
		// If proposer is still preparing the view, the batch builder
		// is not created yet.
		if (_state == ProposerState.PREPARED) {
			batchBuilder.enqueueRequests();
		}
//		sendNextProposal();
	}



	/**
	 * Called to inform the proposer that a decision was taken. Allows the
	 * proposer to make a new proposal.
	 */
	public void ballotFinished() {
		assert _paxos.getDispatcher().amIInDispatcher();

		// There's a space on the window. 
		// Try sending the current batch
		
		// During prepare phase the new leader might decide the
		// instances that were sent on PrepareOK messages. As 
		// part of the normal decision process, it will call this
		// method. In this case, the batchBuilder is still null,
		// so this method shouldn't try to access it. It's also
		// not necessary, as the leader should not issue proposals
		// while preparing. 		
		if (_state == ProposerState.PREPARED) {
			batchBuilder.enqueueRequests();
		}
	}

	// TODO: JK Is this (↓) any longer needed?
	
//	private int lastRetransmitted = 0;
//
//	private void retransmitGaps() {
//
//		// Check if there are gaps on the decisions and retransmit
//		// those messages
//		Log log = _storage.getLog();
//
//		lastRetransmitted = Math.max(lastRetransmitted, _storage.getFirstUncommitted());
//		int lastCommitted = log.getNextId() - 1;
//		while (lastCommitted > lastRetransmitted && log.getState(lastCommitted) != LogEntryState.DECIDED) {
//			lastCommitted--;
//		}
//
//		// _logger.info("lastRetransmitted: " + lastRetransmitted +
//		// ", lastCommitted: " + lastCommitted);
//		for (int i = lastRetransmitted; i < lastCommitted; i++) {
//			RetransmittedMessage handler = _proposeRetransmitters.get(i);
//			if (handler != null) {
//				handler.forceRetransmit();
//			}
//		}
//		lastRetransmitted = lastCommitted;
//	}


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
		_proposeRetransmitters.put(instance.getId(), _retransmitter
		                           .startTransmitting(m, _storage.getAcceptors()));
	}

	/**
	 * As the process looses leadership, it must stop all message retransmission
	 * - that is either prepare or propose messages.
	 */
	public void stopProposer() {
		_state = ProposerState.INACTIVE;
		_pendingProposals.clear();
		if (batchBuilder != null) {
			batchBuilder.cancel();
			batchBuilder = null;
		}

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

		// _logger.info("stopPropose. Instance: "+instanceId + ", size: " +
		// _proposeRetransmitters.size());
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

		// _logger.info("stopPropose. Instance: "+instanceId + ", dest: " +
		// destination + ", size: " + _proposeRetransmitters.size());
		// if (_logger.isLoggable(Level.FINE)) {
		// _logger.fine("Stop sending to " + destination);
		// }
		_proposeRetransmitters.get(instanceId).stop(destination);
	}

	private final static Logger _logger = Logger.getLogger(ProposerImpl.class
	                                                       .getCanonicalName());

	
	final class BatchBuilder implements Runnable {
		/** Used to build the batch */
		final private ArrayList<Request> batchReqs = new ArrayList<Request>(16);
		/** The header takes 4 bytes */
		private int batchSize = 4;
		
		/** If the batch is ready to be sent */
		private boolean ready = false;
		/** If the batch was sent or canceled */
		private boolean cancelled = false;
		/** Builds the string with the log message */
		private final StringBuilder sb;
		
		
		public BatchBuilder() {
			// avoid creating object if log message is not going to be written
			/* WARNING: during shutdown, the LogManager runs a shutdown hook that
			 * resets all loggers, setting their level to null. The root logger
			 * remains at INFO. Therefore, calls to isLoggable(level), with
			 * level >= INFO might start returning true, even if the log level
			 * was set to WARNING or higher. 
			 * This behavior caused a NPE in the code below. The sb was not initialized 
			 * because when this object is created the _logger.isLoggable(Level.INFO) 
			 * returns false, but is later accessed when isLoggable(Level.INFO) return
			 * true. (during shutdown). 
			 * To avoid this, we check if sb == null before trying to access it.
			 */     
			if (_logger.isLoggable(Level.INFO)) {
				sb = new StringBuilder(64);
				sb.append("Proposing: ").append(_storage.getLog().getNextId());
			} else {
				sb = null;
			}
		}

		public void cancel() {
			this.cancelled = true;
		}

		/**
		 * 
		 * @param req
		 * @return true if request was batched, false otherwise.
		 * If method returns false, the request could not be batched
		 * because it would exceed the size limit for a batch. 
		 */
		public void enqueueRequests() {
			if (_pendingProposals.isEmpty()) {
				_logger.fine("enqueueRequests(): No proposal available.");
				return;
			}
			
			while (!_pendingProposals.isEmpty()) {
				Request request = _pendingProposals.getFirst();

				
				if (batchReqs.isEmpty()) {
					int delay = ProcessDescriptor.getInstance().maxBatchDelay;
					if (delay <= 0) {
						// Do not schedule the task if delay is 0
						ready = true;
					} else {
						// Schedule this task for execution within MAX_DELAY time
						_paxos.getDispatcher().schedule(this, Priority.High, delay);
					}
				}
				
				/* If the batch is empty, add the request unconditionally. This is to handle 
				 * requests bigger than the batchingLevel, we have to exceed the 
				 * limit otherwise the request would not be ordered. 
				 */				
				if (batchSize + request.byteSize() <= _paxos.getProcessDescriptor().batchingLevel || 
						batchReqs.isEmpty()) 
				{
					batchSize += request.byteSize();
					batchReqs.add(request);
					_pendingProposals.removeFirst();
					// See comment on constructor for sb!=null
					if (sb != null && _logger.isLoggable(Level.FINE)) {
						sb.append(request.getRequestId().toString()).append(",");						
					}
				} else {
					// no space for next request and batchReqs not empty. Send the batch
					ready = true;
					break;
				}
			}
			
			if (batchSize >= _paxos.getProcessDescriptor().batchingLevel) {
				ready = true;
			}
			
			trySend();
		}
		
		public void trySend() {
//			_logger.info("trySend()");
			int nextID = _storage.getLog().getNextId();
			
			if (batchReqs.isEmpty() || !_storage.isInWindow(nextID)) {
				// Nothing to send or window is full			
				return;
			}

			// If no instance is running, always send
			// Otherwise, send if the batch is ready.
			if (_storage.isIdle() || ready) {
//			if (ready) {
				send();
			}
		}
		
		@Override
		public void run() {
			if (cancelled) {
				return;
			}
			
			// Deadline expired. Should not delay batch any further.			
			ready = true;
			trySend();
		}
		
		private void send() {
			// Can send proposal
			ByteBuffer bb = ByteBuffer.allocate(batchSize);			
			bb.putInt(batchReqs.size());
			for (Request req : batchReqs) {
				req.writeTo(bb);
			}
			byte[] value = bb.array();

			ConsensusInstance instance = 
				_storage.getLog().append(_stableStorage.getView(), value);

			assert _proposeRetransmitters.containsKey(instance.getId()) == false : "Different proposal for the same instance";

			// See comment on constructor for sb!=null			
			if (sb != null) {
				sb.append(" Size:").append(value.length);
				sb.append(", k=").append(batchReqs.size());
				_logger.info(sb.toString());
			}
			
			{
				int alpha = instance.getId() - _storage.getFirstUncommitted() + 1; 
				ReplicaStats.getInstance().consensusStart(
				     instance.getId(), value.length, batchReqs.size(), alpha);
			}
			
			// creating retransmitter, which automatically starts
			// sending propose message to all acceptors
			Message message = new Propose(instance);
			BitSet destinations = _storage.getAcceptors();

			// Mark the instance as accepted locally
			instance.getAccepts().set(_storage.getLocalId());
			// Do not send propose message to self.
			destinations.clear(_storage.getLocalId());

			_proposeRetransmitters.put(instance.getId(), _retransmitter
			                           .startTransmitting(message, destinations));
 
			// If this task was not yet executed by the dispatcher,
			// this flag ensures that the message is not sent twice.
			cancelled = true;
			// Proposer can start a new batch
			ProposerImpl.this.batchBuilder = new BatchBuilder();
		}
	}
}