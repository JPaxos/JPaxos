package lsr.paxos;

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * Represents part of paxos which is responsible for responding on the
 * <code>Prepare</code> message, and also sending <code>Accept</code> after
 * receiving proper <code>Propose</code>.
 */
class Acceptor {
	private final Paxos _paxos;
	private final Storage _storage;
	private final Network _network;

	/**
	 * Initializes new instance of <code>Acceptor</code>.
	 * 
	 * @param paxos
	 *            - the paxos the acceptor belong to
	 * @param storage
	 *            - data associated with the paxos
	 * @param network
	 *            - used to send responses
	 * 
	 */
	public Acceptor(Paxos paxos, Storage storage, Network network) {
		_paxos = paxos;
		_storage = storage;
		_network = network;
	}

	/**
	 * Promises not to accept a proposal numbered less than message view. Sends
	 * the proposal with the highest number less than message view that it has
	 * accepted if any. If message view equals current view, then it may be a
	 * retransmission or out-of-order delivery. If the process already accepted
	 * this proposal, then the proposer doesn't need anymore the prepareOK
	 * message. Otherwise it might need the message, so resent it.
	 * 
	 * @param message
	 *            received prepare message
	 * @see Prepare
	 */
	public void onPrepare(Prepare message, int sender) {
		// assert message.getView() == _storage.getView() : "Msg.view: " +
		// message.getView() + ", view: "
		// + _storage.getView();
		assert _paxos.getDispatcher().amIInDispatcher() : "Thread should not be here: " + Thread.currentThread();

		// TODO:
		// If the message is from the current view:
		// a) This process already received a proposal from this view ->
		// then the proposer already completed the prepare phase and doesn't
		// need the PrepareOK.
		// b) No proposal was received for current view - then this is a
		// retransmission. Must
		// 
		// Currently: always retransmit. The proposer will ignore old
		// PrepareOK messages.
		// Do not send message, since it can be quite large in some
		// cases.
		// Possible implementations:
		// - check if the topmost entry on the log is already stamped with
		// the current view.
		// - Keep a flag associated with the view indicating if a proposal
		// was already received for the current view.

		Log log = _storage.getLog();

		if (message.getFirstUncommitted() < log.getLowestAvailableId()) {
			// We're MUCH MORE up-to-date than the replica that sent Prepare
			_paxos.startProposer();
			return;
		}

		ConsensusInstance[] v = new ConsensusInstance[Math.max(log.getNextId() - message.getFirstUncommitted(), 0)];
		for (int i = message.getFirstUncommitted(); i < log.getNextId(); i++)
			v[i - message.getFirstUncommitted()] = log.getInstance(i);

		PrepareOK m = new PrepareOK(message.getView(), v);
		_network.sendMessage(m, sender);
	}

	/**
	 * Accepts proposals higher or equal than the current view.
	 * 
	 * @param message
	 *            - received propose message
	 * @param sender
	 *            - the id of replica that send the message
	 */
	public void onPropose(Propose message, int sender) {
		// TODO: What if received a proposal for a higher view?
		assert message.getView() == _storage.getStableStorage().getView() : "Msg.view: " + message.getView()
				+ ", view: " + _storage.getStableStorage().getView();
		assert _paxos.getDispatcher().amIInDispatcher() : "Thread should not be here: " + Thread.currentThread();
		ConsensusInstance instance = _storage.getLog().getInstance(message.getInstanceId());
		// The propose is so old, that it's log has already been erased
		if (instance == null) {
			_logger.fine("Ignoring old message: " + message);
			return;
		}

		instance.setValue(message.getView(), message.getValue());
		if (_logger.isLoggable(Level.FINE)) {
			_logger.fine("Instance " + instance.getId());
		}

		// leader will not send the accept message;
		if (!_paxos.isLeader()) {
			// Do not send ACCEPT if there are old instances unresolved
			int firstUncommitted = _paxos.getStorage().getFirstUncommitted();
			int wndSize = _paxos.getProcessDescriptor().windowSize;
			if (firstUncommitted + wndSize < message.getInstanceId()) 
			{
				_logger.info("Instance " + message.getInstanceId() + " out of window.");
				
				if (firstUncommitted + wndSize*2 < message.getInstanceId()) {
					// Assume that message is lost. Execute catchup with normal priority
					_paxos.getCatchup().forceCatchup();					
				} else {
					// Message may not be lost, but try to execute catchup if idle
					_paxos.getCatchup().startCatchup();
				}
				
			} else {
				BitSet destinations = _storage.getAcceptors();
				// Do not send ACCEPT to self
				destinations.clear(_storage.getLocalId());
				_network.sendMessage(new Accept(message), destinations);
			}
		}

		// Might have enough accepts to decide.
		if (instance.getState() == LogEntryState.DECIDED) {			
			if (_logger.isLoggable(Level.FINEST)) {
				_logger.fine("Instance already decided: " + message.getInstanceId());
			}
		} else {			
			// The local process accepts immediately the proposal, 
			// avoids sending an accept message.
			instance.getAccepts().set(_storage.getLocalId());
			// The propose message works as an implicit accept from the leader.
			instance.getAccepts().set(sender);
			if (instance.isMajority(_storage.getN())) {
				_paxos.decide(instance.getId());
			}
		}
	}
	
	private final static Logger _logger = Logger.getLogger(Acceptor.class.getCanonicalName());
}
