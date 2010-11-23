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
	final ProposerImpl proposer;
	private final Acceptor acceptor;
	private final Learner learner;
	private final DecideCallback decideCallback;

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

	final Dispatcher dispatcher;

	final Storage storage;
	private final StableStorage stableStorage;
	private final Network network;
	final FailureDetector failureDetector;
	private final CatchUp catchUp;
	private final SnapshotMaintainer snapshotMaintainer;

	private final Batcher batcher;

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
		this.decideCallback = decideCallback;
		// Create storage
		// All processes are acceptors and learners
		// BitSet bs = new BitSet();
		// bs.set(0, p.config.getN());
		// BitSet learners = bs;
		// BitSet acceptors = bs;
		// StableStorage stableStorage = new UnstableStorage();
		// _storage = new SimpleStorage(stableStorage, p, acceptors, learners);
		this.storage = storage;
		this.stableStorage = storage.getStableStorage();

		// receives messages from the other processes.
		ProcessDescriptor p = ProcessDescriptor.getInstance();
		
		// Just for statistics, not needed for correct execution.
		// TODO: Conditional compilation or configuration flag
		// to disable this code.
		ReplicaStats.initialize(storage.getN(), p.localID);

		// Handles the replication protocol and writes messages to the network
		// _dispatcher = new Dispatcher("Dispatcher");
		dispatcher = new Dispatcher("Dispatcher");
		dispatcher.setBusyThreshold(p.busyThreshold);
		dispatcher.start();

		if (snapshotProvider != null) {
			_logger.info("Starting snapshot maintainer");
			snapshotMaintainer = new SnapshotMaintainer(this.storage, dispatcher, snapshotProvider);
			storage.getStableStorage().getLog().addLogListener(snapshotMaintainer);
		} else {
			_logger.info("No snapshot support");
			snapshotMaintainer = null;
		}

		// UDPNetwork is always needed because of the failure detector
		UdpNetwork udp = new UdpNetwork();
		if (p.network.equals("TCP")) {
			network = new TcpNetwork();
		} else if (p.network.equals("UDP")) {
			network = udp;
		} else if (p.network.equals("Generic")) {
			 TcpNetwork tcp = new TcpNetwork();
			 network = new GenericNetwork(tcp, udp);
		} else {
			throw new IllegalArgumentException("Unknown network type: " + p.network + ". Check paxos.properties configuration.");
		}
		_logger.info("Network: " + network.getClass().getCanonicalName());

		catchUp = new CatchUp(snapshotProvider, this, this.storage, network);
		failureDetector = new FailureDetector(this, udp, this.storage);

		// create acceptors and learners
		proposer = new ProposerImpl(this, network, failureDetector, this.storage);
		acceptor = new Acceptor(this, this.storage, network);
		learner = new Learner(this, proposer, this.storage, network);

		// Batching utility
		batcher = new BatcherImpl(ProcessDescriptor.getInstance().batchingLevel);
	}

	public void startPaxos() {
		// Create Catch-up thread
		catchUp.start();

		// Create failure detector
		failureDetector.start();

		MessageHandler handler = new MessageHandlerImpl();
		Network.addMessageListener(MessageType.Alive, handler);
		Network.addMessageListener(MessageType.Propose, handler);
		Network.addMessageListener(MessageType.Prepare, handler);
		Network.addMessageListener(MessageType.PrepareOK, handler);
		Network.addMessageListener(MessageType.Accept, handler);
	}

	public void propose(Request value) throws NotLeaderException {
		if (!isLeader()) {
			throw new NotLeaderException("Cannot propose: local process is not the leader");
		}
		dispatcher.dispatch(new ProposeEvent(proposer, value));
	}

	/**
	 * Adds {@link StartProposerEvent} to current dispatcher which starts the
	 * proposer on current replica.
	 */
	public void startProposer() {
		assert proposer.getState() == ProposerState.INACTIVE : "Already in proposer role.";

		StartProposerEvent evt = new StartProposerEvent(proposer);
		if (dispatcher.amIInDispatcher()) {
			evt.run();
		} else {
			// _dispatcher.execute(new StartProposerEvent(_proposer));
			dispatcher.dispatch(evt);
		}
	}

	/**
	 * Is this process on the role of leader?
	 * 
	 * @return <code>true</code> if current process is the leader;
	 *         <code>false</code> otherwise
	 */
	public boolean isLeader() {
		return getLeaderId() == storage.getLocalId();
	}

	/**
	 * Gets the id of the replica which is currently the leader.
	 * 
	 * @return id of replica which is leader
	 */
	public int getLeaderId() {
		return stableStorage.getView() % storage.getN();
	}

	/**
	 * Gets the dispatcher used by paxos to avoid concurrency in handling
	 * events.
	 * 
	 * @return current dispatcher class
	 */
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	public void decide(int instanceId) {
		assert dispatcher.amIInDispatcher() : "Incorrect thread: " + Thread.currentThread();

		ConsensusInstance ci = storage.getLog().getInstance(instanceId);
		assert ci != null : "Deciding on instance already removed from logs";
		assert ci.getState() != LogEntryState.DECIDED : "Deciding on already decided instance";

		ci.setDecided();

		if (_logger.isLoggable(Level.INFO)) {
			_logger.info("Decided " + instanceId + ", Log Size: " + storage.getLog().size());
		}

		ReplicaStats.getInstance().consensusEnd(instanceId);
		storage.updateFirstUncommitted();

		if (isLeader()) {
			proposer.stopPropose(instanceId);
			proposer.ballotFinished();
		} else {
			// not leader. Should we start the catchup?
			if (ci.getId() > storage.getFirstUncommitted() + ProcessDescriptor.getInstance().windowSize) {
				// The last uncommitted value was already decided, since
				// the decision just reached is outside the ordering window
				// So start catchup.
				catchUp.startCatchup();
			}
		}

		Deque<Request> requests = batcher.unpack(ci.getValue());
		decideCallback.onRequestOrdered(instanceId, requests);
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
		assert dispatcher.amIInDispatcher();
		assert newView > stableStorage.getView() : "Can't advance to the same or lower view";

		_logger.info("Advancing to view " + newView + ", Leader=" + (newView % storage.getN()));

		ReplicaStats.getInstance().advanceView(newView);

		if (isLeader()) {
			proposer.stopProposer();
		}

		/* TODO: NS [FullSS] don't sync to disk at this point.
		 */
		stableStorage.setView(newView);

		assert !isLeader() : "Cannot advance to a view where process is leader by receiving a message";
		failureDetector.leaderChange(getLeaderId());
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
				dispatcher.dispatch(event, Priority.High);
			} else {
				dispatcher.dispatch(event);
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
				if (msg.getView() < stableStorage.getView())
					return;

				// TODO: check correctness of moving this code here.
				if (msg.getView() > stableStorage.getView()) {
					assert msg.getType() != MessageType.PrepareOK : "Received PrepareOK for view " + msg.getView()
							+ " without having sent a Prepare";
					advanceView(msg.getView());
				}

				// Invariant for all message handlers: msg.view >= view
				switch (msg.getType()) {
					case Prepare:
						acceptor.onPrepare((Prepare) msg, sender);
						break;

					case PrepareOK:
						if (proposer.getState() == ProposerState.INACTIVE) {
							_logger.fine("Not in proposer role. Ignoring message");
						} else {
							proposer.onPrepareOK((PrepareOK) msg, sender);
						}
						break;

					case Propose:
						acceptor.onPropose((Propose) msg, sender);
						if (!storage.isInWindow(((Propose) msg).getInstanceId()))
							activateCatchup();
						// if (proposeOutsideWindow())
						// activateCatchup();
						break;

					case Accept:
						learner.onAccept((Accept) msg, sender);
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
			Log log = storage.getLog();

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
			int i = storage.getFirstUncommitted();
			for (; i < log.getNextId() - ProcessDescriptor.getInstance().windowSize; i++) {
				// if (log.getInstance(i) == null) // happens after snapshot
				// catch up
				// continue;
				if (log.getInstance(i).getState() != LogEntryState.DECIDED)
					return true;
			}
			return false;

		}

		private void activateCatchup() {
			synchronized (catchUp) {
				catchUp.notify();
			}
		}
	}

	public void onSnapshotMade(Snapshot snapshot) {
		snapshotMaintainer.onSnapshotMade(snapshot);
	}

	final static Logger _logger = Logger.getLogger(PaxosImpl.class.getCanonicalName());

	public Storage getStorage() {
		return storage;
	}

	@Override
	public CatchUp getCatchup() {
		return catchUp;
	}
}
