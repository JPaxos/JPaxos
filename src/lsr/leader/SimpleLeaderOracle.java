package lsr.leader;

import java.util.BitSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import lsr.common.KillOnExceptionHandler;
import lsr.leader.messages.SimpleAlive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;

/**
 * A simple leader election algorithm. The leader sends <code>ALIVE</code> to
 * all processes periodically. When some process stops receiving the
 * <code>ALIVE</code> messages, it suspects the leader and tries to become the
 * leader. Processes keep a internal view number. The current leader is always
 * process <code>view % n</code>.
 */
public class SimpleLeaderOracle implements LeaderOracle {
	/** How long to wait until suspecting the leader. In milliseconds */
	private final static int SUSPECT_LEADER = 2000;
	/** How long the leader waits until sending heartbeats. In milliseconds */
	private final static int SEND_TIMEOUT = 1000;

	private final Network network;
	private LeaderOracleListener listener;

	private final int localID;
	private final int n;

	/** Receives notifications of messages from the network class */
	private InnerMessageHandler innerHandler;

	/** The current view. The leader is (view % N) */
	int view = -1;

	/** The name of the leader oracle thread */
	private final static String LO_THREAD_NAME = "LeaderOracle";
	/** Thread that runs all operations related to leader election */
	final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
			1, new ThreadFactory() {
				public Thread newThread(Runnable r) {
					// Name the thread for debugging.
					Thread t = new Thread(r, LO_THREAD_NAME);
					t.setUncaughtExceptionHandler(new KillOnExceptionHandler());
					return t;
				}
			});

	/** Sends pings, used when this process is on the leader role */
	private ScheduledFuture<SendAlivesTask> sendTask;
	/** Executed when the timeout on the leader expires. */
	private ScheduledFuture<SuspectLeaderTask> suspectTask;

	/**
	 * Initializes new instance of <code>FailureDetector</code>.
	 * 
	 * @param paxos
	 *            - the paxos which should be notified about suspecting leader
	 * @param network
	 *            - used to send and receive messages
	 * @param storage
	 *            - storage containing all data about paxos
	 */
	public SimpleLeaderOracle(Network network, int localID, int N) {
		this.network = network;
		innerHandler = new InnerMessageHandler();
		// Register interest in receiving Alive messages
		Network.addMessageListener(MessageType.SimpleAlive, innerHandler);
		this.localID = localID;
		this.n = N;
	}

	final public void start() {
		if (view != -1) {
			throw new RuntimeException("Already started");
		}
		// Initiate the first view, already using the executor thread.
		executor.execute(new Runnable() {
			public void run() {
				advanceView(0);
			}
		});
	}

	public void stop() {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	private void startSendTask() {
		checkIsInExecutorThread();
		if (sendTask != null) {
			sendTask.cancel(true);
		}
		// Repeat execution
		sendTask = (ScheduledFuture<SendAlivesTask>) executor
				.scheduleAtFixedRate(new SendAlivesTask(), 0, SEND_TIMEOUT,
						TimeUnit.MILLISECONDS);
	}

	/**
	 * Executed when a process receives a message from the leader (
	 * <code>ALIVE</code> or protocol message)
	 */
	@SuppressWarnings("unchecked")
	private void resetFollowerTimer() {
		checkIsInExecutorThread();

		if (suspectTask != null) {
			suspectTask.cancel(true);
		}
		// Schedule for single execution.
		suspectTask = (ScheduledFuture<SuspectLeaderTask>) executor.schedule(
				new SuspectLeaderTask(), SUSPECT_LEADER, TimeUnit.MILLISECONDS);
	}

	/** Called to advance view */
	void advanceView(int newView) {
		checkIsInExecutorThread();

		// If this process is the leader for the current view, stop sending
		// hearbeats
		if (sendTask != null) {
			sendTask.cancel(true);
			sendTask = null;
		}
		this.view = newView;
		_logger.info("New view: " + newView + " leader: " + getLeader());
		// _logger.info("Advancing to view: " + newView + " (Leader: " +
		// getLeader());

		// Am I the leader?
		if (isLeader()) {
			startSendTask();
			_logger.fine("I'm leader now.");
		}

		// Reset the suspect timeout
		resetFollowerTimer();

		// Notify the listener
		listener.onNewLeaderElected(getLeader());
	}

	private void onAlive(Message msg, int sender) {
		checkIsInExecutorThread();
		assert msg.getView() % n == sender : "Alive for view " + msg.getView()
				+ " sent by process " + sender;
		SimpleAlive alive = (SimpleAlive) msg;
		if (msg.getView() > view) {
			advanceView(alive.getView());
		} else if (msg.getView() == view) {
			// Reset timer
			resetFollowerTimer();
		}
	}

	final class SendAlivesTask implements Runnable {
		public void run() {
			checkIsInExecutorThread();

			SimpleAlive alive = new SimpleAlive(view);
			network.sendToAll(alive);
		}
	}

	final class SuspectLeaderTask implements Runnable {
		/**
		 * Local process tries to become the leader.
		 */
		public void run() {
			assert !isLeader() : "Process suspected itself!";
			checkIsInExecutorThread();

			_logger.warning("Suspecting leader: " + getLeader());
			// increment view until reaching the next view where the local
			// process
			// is the leader
			int newView = view++;
			while (newView % n != localID) {
				newView++;
			}
			advanceView(newView);
		}
	}

	final class InnerMessageHandler implements MessageHandler {
		public void onMessageReceived(final Message msg, final int sender) {
			// Execute on the dispatcher thread.
			executor.execute(new Runnable() {
				public void run() {
					onAlive(msg, sender);
				}
			});
		}

		public void onMessageSent(Message message, BitSet destinations) {
			// Empty
		}
	}

	public int getLeader() {
		return view % n;
	}

	public boolean isLeader() {
		return getLeader() == localID;
	}

	public void registerLeaderOracleListener(LeaderOracleListener listener) {
		if (this.listener != null) {
			throw new RuntimeException(
					"Leader oracle listener already registered: " + this.listener);
		}
		this.listener = listener;
	}

	public void removeLeaderOracleListener(LeaderOracleListener listener) {
		if (this.listener != listener) {
			throw new RuntimeException(
					"Cannot unregister: listener no registered (" + listener
							+ ")");
		}
		this.listener = null;
	}

	/** Sanity checks */
	private final static void checkIsInExecutorThread() {
		assert Thread.currentThread().getName().equals(LO_THREAD_NAME);
	}

	private final static Logger _logger = Logger
			.getLogger(SimpleLeaderOracle.class.getCanonicalName());

	public int getDelta() {
		return 0;
	}

}