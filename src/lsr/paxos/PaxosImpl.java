package lsr.paxos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.Dispatcher.Priority;
import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.paxos.Proposer.ProposerState;
import lsr.paxos.events.ProposeEvent;
import lsr.paxos.events.StartProposerEvent;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.GenericNetwork;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.network.TcpNetwork;
import lsr.paxos.network.UdpNetwork;
import lsr.paxos.statistics.ReplicaStats;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

/**
 * Implements state machine replication. It keeps a replicated log internally
 * and informs the listener of decisions using callbacks. This implementation is
 * monolithic, in the sense that leader election/view change are integrated on
 * the paxos protocol.
 * 
 * <p>
 * Crash-stop model.
 * </p>
 * 
 * <p>
 * The first consensus instance is 0. Decisions might not be reached in sequence
 * number order.
 * </p>
 */
public class PaxosImpl implements Paxos {
	final ProposerImpl _proposer;
	private final Acceptor _acceptor;
	private final Learner _learner;
	private final DecideCallback _decideCallback;

	/**
	 * Threading model - This class uses an event-driven threading model. It
	 * starts a Dispatcher thread that is responsible for executing the
	 * replication protocol and has exclusive access to the internal data
	 * structures. The Dispatcher receives work using the pendingEvents queue.
	 */
	/**
	 * The Dispatcher thread executes the replication protocol. It receives and
	 * executes events placed on the pendingEvents queue: messages from other
	 * processes or proposals from the local process.
	 * 
	 * Only this thread is allowed to access the state of the replication
	 * protocol. Therefore, there is no need for synchronization when accessing
	 * this state. The synchronization is handled by the
	 * <code>pendingEvents</code> queue.
	 */
	// final SingleThreadDispatcher _dispatcher;

	final Dispatcher _dispatcher;

	final Storage _storage;
	private final StableStorage _stableStorage;
	private final Network _network;
	final FailureDetector _failureDetector;
	private final CatchUp _catchUp;
	private final SnapshotMaintainer _snapshotMaintainer;

	private final Batcher _batcher;

	/**
	 * 
	 * @param localId
	 * @param processes
	 * @param decideCallback
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public PaxosImpl(DecideCallback decideCallback, Storage storage) throws IOException {
		this(decideCallback, null, storage);
	}

	/**
	 * Initializes new instance of {@link PaxosImpl}.
	 * 
	 * @param localId
	 *            The id of the local process
	 * @param processes
	 *            The list of all processes on the group. This list must be
	 *            exactly the same in all replicas of the group
	 * @param decideCallback
	 *            The class that should be notified of decisions.
	 * @param snapshotProvider
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public PaxosImpl(DecideCallback decideCallback, SnapshotProvider snapshotProvider,
			Storage storage) throws IOException {
		this._decideCallback = decideCallback;
		// Create storage
		// All processes are acceptors and learners
		// BitSet bs = new BitSet();
		// bs.set(0, p.config.getN());
		// BitSet learners = bs;
		// BitSet acceptors = bs;
		// StableStorage stableStorage = new UnstableStorage();
		// _storage = new SimpleStorage(stableStorage, p, acceptors, learners);
		this._storage = storage;
		this._stableStorage = storage.getStableStorage();

		// receives messages from the other processes.
		ProcessDescriptor p = ProcessDescriptor.getInstance();
		
		// Just for statistics, not needed for correct execution.
		// TODO: Conditional compilation or configuration flag
		// to disable this code.
		ReplicaStats.initialize(storage.getN(), p.localID);

		// Handles the replication protocol and writes messages to the network
		// _dispatcher = new Dispatcher("Dispatcher");
		_dispatcher = new Dispatcher("Dispatcher");
		_dispatcher.setBusyThreshold(p.busyThreshold);
		_dispatcher.start();

		if (snapshotProvider != null) {
			_logger.info("Starting snapshot maintainer");
			_snapshotMaintainer = new SnapshotMaintainer(_storage, _dispatcher, snapshotProvider);
			storage.getStableStorage().getLog().addLogListener(_snapshotMaintainer);
		} else {
			_logger.info("No snapshot support");
			_snapshotMaintainer = null;
		}

		// UDPNetwork is always needed because of the failure detector
		UdpNetwork udp = new UdpNetwork(p);
		if (p.network.equals("TCP")) {
			_network = new TcpNetwork(p);
		} else if (p.network.equals("UDP")) {
			_network = udp;
		} else if (p.network.equals("Generic")) {
			 TcpNetwork tcp = new TcpNetwork(p);
			 _network = new GenericNetwork(p, tcp, udp);
		} else {
			throw new IllegalArgumentException("Unknown network type: " + p.network + ". Check paxos.properties configuration.");
		}
		_logger.info("Network: " + _network.getClass().getCanonicalName());

		_catchUp = new CatchUp(snapshotProvider, this, _storage, _network);
		_failureDetector = new FailureDetector(this, udp, _storage);

		// create acceptors and learners
		_proposer = new ProposerImpl(this, _network, _failureDetector, _storage);
		_acceptor = new Acceptor(this, _storage, _network);
		_learner = new Learner(this, _proposer, _storage, _network);

		// Batching utility
		_batcher = new BatcherImpl(ProcessDescriptor.getInstance().batchingLevel);
	}

	public void startPaxos() {
		// Create Catch-up thread
		_catchUp.start();

		// Create failure detector
		_failureDetector.start();

		MessageHandler handler = new MessageHandlerImpl();
		_network.addMessageListener(MessageType.Alive, handler);
		_network.addMessageListener(MessageType.Propose, handler);
		_network.addMessageListener(MessageType.Prepare, handler);
		_network.addMessageListener(MessageType.PrepareOK, handler);
		_network.addMessageListener(MessageType.Accept, handler);
	}

	public void propose(Request value) throws NotLeaderException {
		if (!isLeader()) {
			throw new NotLeaderException("Cannot propose: local process is not the leader");
		}
		_dispatcher.dispatch(new ProposeEvent(_proposer, value));
	}

	/**
	 * Adds {@link StartProposerEvent} to current dispatcher which starts the
	 * proposer on current replica.
	 */
	public void startProposer() {
		assert _proposer.getState() == ProposerState.INACTIVE : "Already in proposer role.";

		StartProposerEvent evt = new StartProposerEvent(_proposer);
		if (_dispatcher.amIInDispatcher()) {
			evt.run();
		} else {
			// _dispatcher.execute(new StartProposerEvent(_proposer));
			_dispatcher.dispatch(evt);
		}
	}

	/**
	 * Is this process on the role of leader?
	 * 
	 * @return <code>true</code> if current process is the leader;
	 *         <code>false</code> otherwise
	 */
	public boolean isLeader() {
		return getLeaderId() == _storage.getLocalId();
	}

	/**
	 * Gets the id of the replica which is currently the leader.
	 * 
	 * @return id of replica which is leader
	 */
	public int getLeaderId() {
		return _stableStorage.getView() % _storage.getN();
	}

	/**
	 * Gets the dispatcher used by paxos to avoid concurrency in handling
	 * events.
	 * 
	 * @return current dispatcher class
	 */
	public Dispatcher getDispatcher() {
		return _dispatcher;
	}

	public void decide(int instanceId) {
		assert _dispatcher.amIInDispatcher() : "Incorrect thread: " + Thread.currentThread();

		ConsensusInstance ci = _storage.getLog().getInstance(instanceId);
		assert ci != null : "Deciding on instance already removed from logs";
		assert ci.getState() != LogEntryState.DECIDED : "Deciding on already decided instance";

		ci.setDecided();

		if (_logger.isLoggable(Level.INFO)) {
			_logger.info("Decided " + instanceId + ", Log Size: " + _storage.getLog().size());
		}

		ReplicaStats.getInstance().consensusEnd(instanceId);
		_storage.updateFirstUncommitted();

		if (isLeader()) {
			_proposer.stopPropose(instanceId);
			_proposer.ballotFinished();
		} else {
			// not leader. Should we start the catchup?
			if (ci.getId() > _storage.getFirstUncommitted() + ProcessDescriptor.getInstance().windowSize) {
				// The last uncommitted value was already decided, since
				// the decision just reached is outside the ordering window
				// So start catchup.
				_catchUp.startCatchup();
			}
		}

		Deque<Request> requests = _batcher.unpack(ci.getValue());
		_decideCallback.onRequestOrdered(instanceId, requests);
	}

	public List<Request> extractValueList(byte[] value) {
		ByteBuffer bb = ByteBuffer.wrap(value);
		int count = bb.getInt();
		List<Request> requests = new ArrayList<Request>(count);
		for (int i = 0; i < count; ++i) {
			requests.add(Request.create(bb));
		}
		return requests;
	}

	public void advanceView(int newView) {
		assert _dispatcher.amIInDispatcher();
		assert newView > _stableStorage.getView() : "Can't advance to the same or lower view";

		_logger.info("Advancing to view " + newView + ", Leader=" + (newView % _storage.getN()));

		ReplicaStats.getInstance().advanceView(newView);

		if (isLeader())
			_proposer.stopProposer();

		_stableStorage.setView(newView);

		assert !isLeader() : "Cannot advance to a timestamp where process is leader by receiving a message";
		_failureDetector.leaderChange(getLeaderId());
	}

	// *****************
	// Auxiliary classes
	// *****************
	/**
	 * Receives messages from other processes and stores them on the
	 * pendingEvents queue for processing by the Dispatcher thread.
	 */
	private class MessageHandlerImpl implements MessageHandler {
		public void onMessageReceived(Message msg, int sender) {
			if (_logger.isLoggable(Level.FINEST)) {
				_logger.finest("Msg rcv: " + msg);
			}
			MessageEvent event = new MessageEvent(msg, sender);
			// _dispatcher.queueIncomingMessage(event);
			// Prioritize Alive messages
			if (msg instanceof Alive) {
				_dispatcher.dispatch(event, Priority.High);
			} else {
				_dispatcher.dispatch(event);
			}
		}

		public void onMessageSent(Message message, BitSet destinations) {
			// Empty
		}
	}

	class MessageEvent implements Runnable {
		private final Message msg;
		private final int sender;

		public MessageEvent(Message msg, int sender) {
			this.msg = msg;
			this.sender = sender;
		}

		public void run() {
			try {
				// The monolithic implementation of Paxos does not need Nack
				// messages because the Alive messages from the failure detector
				// are handled by the Paxos algorithm, so it can advance view
				// when it receives Alive messages. But in the modular
				// implementation, the Paxos algorithm does not use Alive
				// messages,
				// so if a process p is on a lower view and the system is idle,
				// p will remain in the lower view until there is another
				// request
				// to be ordered. The Nacks are required to force the process to
				// advance

				// Ignore any message with a lower view.
				if (msg.getView() < _stableStorage.getView())
					return;

				// TODO: check correctness of moving this code here.
				if (msg.getView() > _stableStorage.getView()) {
					assert msg.getType() != MessageType.PrepareOK : "Received PrepareOK for view " + msg.getView()
							+ " without having sent a Prepare";
					advanceView(msg.getView());
				}

				// Invariant for all message handlers: msg.view >= view
				switch (msg.getType()) {
					case Prepare:
						_acceptor.onPrepare((Prepare) msg, sender);
						break;

					case PrepareOK:
						if (_proposer.getState() == ProposerState.INACTIVE) {
							_logger.fine("Not in proposer role. Ignoring message");
						} else {
							_proposer.onPrepareOK((PrepareOK) msg, sender);
						}
						break;

					case Propose:
						_acceptor.onPropose((Propose) msg, sender);
						if (!_storage.isInWindow(((Propose) msg).getInstanceId()))
							activateCatchup();
						// if (proposeOutsideWindow())
						// activateCatchup();
						break;

					case Accept:
						_learner.onAccept((Accept) msg, sender);
						break;

					case Alive:
						// The function checkIfCatchUpNeeded also creates
						// missing
						// logs
						if (!isLeader() && checkIfCatchUpNeeded(((Alive) msg).getLogSize()))
							activateCatchup();
						break;

					default:
						_logger.warning("Unknown message type: " + msg);
				}

				// else if (sender == getLeaderID()) {
				// // Any message from the leader is used to reset the timeout.
				// // Could be an Alive message.
				// _failureDetector.onMsgFromLeader(msg, sender);
				// }
			} catch (Throwable t) {
				t.printStackTrace();
				System.exit(1);
			}
			// _dispatcher.incomingMessageHandled();
		}

		/**
		 * After getting a alive message, we need to check whether we're up to
		 * date
		 * 
		 * @param logSize
		 */
		private boolean checkIfCatchUpNeeded(int logSize) {
			Log log = _storage.getLog();

			if (log.getNextId() < logSize) {

				// If we got information, that a newer instance exists, we can
				// create it
				log.getInstance(logSize - 1);

				// // We may want to run Catch-Up if we're not up to date with
				// // log size
				// return true;
			}

			// // This condition may be used, but currently timeouts are send
			// // also during running ballot
			// // We got alive - that is, there is no consensus running - that
			// // is we should have all decided
			// for (int i = _storage.getFirstUncommitted(); i < log.size(); i++)
			// {
			// if (log.get(i).getState() != LogEntryState.DECIDED)
			// return true;
			// }
			// return false;

			// We check if all ballots outside the window finished
			int i = _storage.getFirstUncommitted();
			for (; i < log.getNextId() - _storage.getWindowSize(); i++) {
				// if (log.getInstance(i) == null) // happens after snapshot
				// catch up
				// continue;
				if (log.getInstance(i).getState() != LogEntryState.DECIDED)
					return true;
			}
			return false;

		}

		private void activateCatchup() {
			synchronized (_catchUp) {
				_catchUp.notify();
			}
		}
	}

	public void onSnapshotMade(Snapshot snapshot) {
		_snapshotMaintainer.onSnapshotMade(snapshot);
	}

	final static Logger _logger = Logger.getLogger(PaxosImpl.class.getCanonicalName());

	public Storage getStorage() {
		return _storage;
	}

	@Override
	public CatchUp getCatchup() {
		return _catchUp;
	}
}
