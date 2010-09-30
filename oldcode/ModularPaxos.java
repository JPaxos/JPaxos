package lsr.paxos;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.common.SingleThreadDispatcher;
import lsr.leader.LeaderOracle;
import lsr.leader.LeaderOracleListener;
import lsr.leader.SimpleLeaderOracle;
import lsr.paxos.Proposer.ProposerState;
import lsr.paxos.events.ProposeEvent;
import lsr.paxos.events.StartProposerEvent;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Nack;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.GenericNetwork;
import lsr.paxos.network.Network;
import lsr.paxos.network.TcpNetwork;
import lsr.paxos.network.UdpNetwork;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.SimpleStorage;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.UnstableStorage;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * Implements state machine replication. It keeps a replicated log internally
 * and informs the listener of decisions using callbacks.
 * 
 * Modular implementation that uses an external leader election module
 * {@link LeaderOracle} to initiate view changes.
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
public class ModularPaxos implements LeaderOracleListener, Paxos {
	private final ModularProposer _proposer;
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
	private final SingleThreadDispatcher _dispatcher;

	private final Storage _storage;
	private final Network _network;
	private final LeaderOracle _leaderOracle;
	private final CatchUp _catchUp;
	private final SnapshotMaintainer _snapshotMaintainer;
	private final ProcessDescriptor p;

	/**
	 * Initializes new instance of {@link ModularPaxos}.
	 * 
	 * @param localId
	 *            The id of the local process
	 * @param processes
	 *            The list of all processes on the group. This list must be
	 *            exactly the same in all replicas of the group
	 * @param decideCallback
	 *            The class that should be notified of decisions.
	 * @param service
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public ModularPaxos(ProcessDescriptor p, DecideCallback decideCallback, SnapshotProvider snapshotProvider)
			throws Exception {
		// Handles the replication protocol and writes messages to the network
		_dispatcher = new SingleThreadDispatcher("Dispatcher");
		_decideCallback = decideCallback;
		this.p = p;

		// Create storage
		// All processes are acceptors and learners
		BitSet bs = new BitSet();
		bs.set(0, p.config.getN());
		BitSet learners = bs;
		BitSet acceptors = bs;
		StableStorage stableStorage = new UnstableStorage();
		_storage = new SimpleStorage(stableStorage, p, acceptors, learners);

		_snapshotMaintainer = new SnapshotMaintainer(_storage, _dispatcher, snapshotProvider);

		stableStorage.getLog().addLogListener(_snapshotMaintainer);

		// receives messages from the other processes.
		UdpNetwork udp = new UdpNetwork(p);
		TcpNetwork tcp = new TcpNetwork(p);
		_network = new GenericNetwork(p, tcp, udp);

		// _network = new UnreliableNetwork(localId, processes);
		// _network = tcp;

		// Create Catch-up thread
		_catchUp = new CatchUp(snapshotProvider, this, _storage, _network);

		// Create failure detector
		_leaderOracle = new SimpleLeaderOracle(_network, _storage.getLocalId(), _storage.getN());
		_leaderOracle.registerLeaderOracleListener(this);

		// create acceptors and learners
		_proposer = new ModularProposer(this, _network, _storage);
		_acceptor = new Acceptor(this, _storage, _network);
		_learner = new Learner(this, _proposer, _storage, _network);
	}

	public void startPaxos() {
		// TODO Please, check the code - didn't tested if it is operational now.
		// Changes made because the recovery phase needs to be separated form
		// starting for Paxos.

		// _network.addNetworkListener(new NetworkListenerImpl());
		MessageReceiver mr = new MessageReceiver();
		_network.addMessageListener(MessageType.Propose, mr);
		_network.addMessageListener(MessageType.Prepare, mr);
		_network.addMessageListener(MessageType.PrepareOK, mr);
		_network.addMessageListener(MessageType.Accept, mr);
		_network.addMessageListener(MessageType.Nack, mr);

		_catchUp.start();
		try {
			_leaderOracle.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Adds {@link ProposeEvent} to current dispatcher which starts proposing
	 * new value by <code>Proposer</code> of this replica. This replica has to
	 * by a leader to call this method.
	 * 
	 * @param value
	 *            - the object to propose in new consensus
	 * @throws InterruptedException
	 */
	public void propose(Request value) throws NotLeaderException {
		if (!isLeader()) {
			throw new NotLeaderException("Cannot propose: local process is not the leader");
		}
		_dispatcher.execute(new ProposeEvent(_proposer, value));
	}

	/**
	 * Adds {@link StartProposerEvent} to current dispatcher which starts the
	 * proposer on current replica.
	 */
	public void startProposer() {
		assert _proposer.getState() == ProposerState.INACTIVE : "Already in proposer role.";

		_logger.info("Queueing Start Proposer event");
		_dispatcher.execute(new StartProposerEvent(_proposer));
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
		return _storage.getStableStorage().getView() % _storage.getN();
	}

	/**
	 * Gets the dispatcher used by paxos to avoid concurrency in handling
	 * events.
	 * 
	 * @return current dispatcher class
	 */
	public SingleThreadDispatcher getDispatcher() {
		return _dispatcher;
	}

	public void decide(int instanceId) {
		assert _dispatcher.amIInDispatcher() : "Incorect thread: " + Thread.currentThread();

		ConsensusInstance ci = _storage.getLog().getInstance(instanceId);
		assert ci != null : "Deciding on instance already removed from logs";
		assert ci.getState() != LogEntryState.DECIDED : "Deciding on already decided instance";

		ci.setDecided();

		_logger.fine("Decided- " + instanceId + ":" + ci);
		_storage.updateFirstUncommitted();

		if (isLeader()) {
			_proposer.stopPropose(instanceId);
			_proposer.ballotFinished();
		}

		List<Request> requests = extractValueList(ci.getValue());
		_decideCallback.onRequestOrdered(instanceId, requests);
	}

	public List<Request> extractValueList(byte[] value) {
		List<Request> requests = new LinkedList<Request>();
		ByteArrayInputStream bais = new ByteArrayInputStream(value);
		DataInputStream dis = new DataInputStream(bais);

		try {
			int count = dis.readInt();
			byte[] b;
			for (int i = 0; i < count; ++i) {
				b = new byte[dis.readInt()];
				dis.readFully(b);
				requests.add(Request.create(b));
			}
		} catch (IOException e) {
			// TODO: decided value is incorrect; cannot deserialize it;
			// BIG ERROR
			throw new RuntimeException(e);
		}

		return requests;
	}

	public void advanceView(int newView) {
		assert _dispatcher.amIInDispatcher();
		assert newView > _storage.getStableStorage().getView() : "Can't advance to the same or lower view";

		_logger.info("Advancing view. From " + _storage.getStableStorage().getView() + " to " + newView + " ("
				+ (newView % _storage.getN()) + ")");

		if (isLeader())
			_proposer.stopProposer();

		_storage.getStableStorage().setView(newView);

		assert !isLeader() : "Cannot advance to a timestamp where process is leader by receiving a message";
	}

	// *****************
	// Auxiliary classes
	// *****************
	/**
	 * Receives messages from other processes and stores them on the
	 * pendingEvents queue for processing by the Dispatcher thread.
	 */
	private class MessageReceiver implements lsr.paxos.network.MessageHandler {
		/** Called by the network */
		public void onMessageReceived(Message msg, int sender) {
			MessageEvent event = new MessageEvent(msg, sender);
			_dispatcher.execute(event);
		}

		public void onMessageSent(Message message, BitSet destinations) {
			// Empty
		}
	}

	private class MessageEvent implements Runnable {
		private final Message msg;
		private final int sender;

		public MessageEvent(Message msg, int sender) {
			this.msg = msg;
			this.sender = sender;
		}

		public void run() {
			_logger.info("Event start: " + msg);
			// If we receive a message from a lower view, send a nack to
			// inform the sender of the new view. Not necessary for correctness
			// but improves the time until all processes join the new view.
			if (msg.getView() < _storage.getStableStorage().getView()) {
				_network.sendMessage(new Nack(_storage.getStableStorage().getView()), sender);
				return;
			}

			if (msg.getView() > _storage.getStableStorage().getView()) {
				assert msg.getType() != MessageType.PrepareOK : "Received PrepareOK for view " + msg.getView()
						+ " without having sent a Prepare";
				advanceView(msg.getView());
			}

			switch (msg.getType()) {
				case Prepare:
					_acceptor.onPrepare((Prepare) msg, sender);
					break;

				case PrepareOK:
					_proposer.onPrepareOK((PrepareOK) msg, sender);
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

				case Nack:
					// Only needed to advance view
					break;
				// case Alive:
				// // The function checkIfCatchUpNeeded also creates missing
				// logs
				// if (!isLeader() && checkIfCatchUpNeeded(((Alive)
				// msg).getLogSize()))
				// activateCatchup();
				// break;

				default:
					_logger.warning("Unexpected message type: " + msg);
					break;
			}

			// else if (sender == getLeaderID()) {
			// // Any message from the leader is used to reset the timeout.
			// // Could be an Alive message.
			// _failureDetector.onMsgFromLeader(msg, sender);
			// }
			_logger.info("Event done: " + msg);
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

	public void writeLog(PrintStream out) {
		Log log = _storage.getStableStorage().getLog();
		out.println("log size: " + log.getNextId());
		out.println("firstUncommitted: " + _storage.getFirstUncommitted());
		for (ConsensusInstance ci : log.getInstanceMap().values()) {
			out.println(ci);
		}
	}

	public void writeDecisions(PrintStream out) {
		for (ConsensusInstance ci : _storage.getLog().getInstanceMap().values()) {
			out.println(ci.getId() + "=" + ci.getValue());
		}
	}

	public void onSnapshotMade(int instance, byte[] snapshot) {
		_snapshotMaintainer.onSnapshotMade(instance, snapshot);

	}

	public Storage getStorage() {
		return _storage;
	}

	public void onNewLeaderElected(int leader) {
		// Ignore any election event for other processes. The process that is
		// elected leader will be the first one to advance to the new view and
		// send a Prepare message to all, which will trigger the view change on
		// the other processes.
		if (leader == this._storage.getLocalId()) {
			startProposer();
		}
		// else {
		// advanceView(newView);
		// }
	}

	private final static Logger _logger = Logger.getLogger(ModularPaxos.class.getCanonicalName());

	public ProcessDescriptor getProcessDescriptor() {
		return p;
	}
}
