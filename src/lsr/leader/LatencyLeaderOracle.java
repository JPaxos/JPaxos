package lsr.leader;

import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import lsr.common.Handler;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.common.Util;
import lsr.leader.latency.LatencyDetector;
import lsr.leader.latency.LatencyDetectorListener;
import lsr.leader.messages.Report;
import lsr.leader.messages.SimpleAlive;
import lsr.leader.messages.Start;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandlerAdapter;
import lsr.paxos.network.Network;

/**
 * Implementation of the latency-aware leader election algorithm
 * 
 * @author Donz? Benjamin
 * @author Nuno Santos
 */
public class LatencyLeaderOracle implements LeaderOracle, LatencyDetectorListener {
    /** Upper bound on transmission time of a message */
    public final static String DELTA = "leader.delta";
    private final int delta;
    private final static int DEFAULT_DELTA = 1000;

    /** Upper bound of the deviation of the transmission time of a message */
    public final static String EPS = "leader.eps";
    private final int eps;
    private final static int DEFAULT_EPS = 1000;

    private final Network network;
    private final CopyOnWriteArrayList<LeaderOracleListener> listeners;
    private final LatencyDetector latencyDetector;

    private final ProcessDescriptor p;
    private final int n;

    /** The estimations of rtt from any host to any host */
    private final double[][] rttMatrix;

    /** The local estimation of rtt. In milliseconds */
    private double[] localRTT;
    /** Receives notifications of messages from the network class */
    private InnerMessageHandler innerHandler;

    /** The current view. The leader is (view % N) */
    int view = -1;

    /**
     * Select Leader and send alive, used when this process is on the leader
     * role
     */
    private ScheduledFuture<SelectAndSendTask> selectSendTask = null;

    /** Executed when the timeout on the leader expires. 3 delta time */
    private ScheduledFuture<SuspectLeaderTask> suspectTask = null;

    /** Thread that runs all operations related to leader election */
    private final SingleThreadDispatcher executor;

    private final static Logger logger = Logger.getLogger(LatencyLeaderOracle.class.getCanonicalName());

    /**
     * Initializes new instance of <code>LeaderElector</code>.
     * 
     * @param network - used to send and receive messages
     * @param localID - the id of this process
     * @param n - the total number of process
     * @param loConfPath - the path of the configuration file
     */
    public LatencyLeaderOracle(ProcessDescriptor p, Network network,
                               SingleThreadDispatcher executor, LatencyDetector latDetector) {
        this.p = p;
        this.n = p.config.getN();
        this.network = network;
        this.executor = executor;
        this.delta = p.config.getIntProperty(DELTA, DEFAULT_DELTA);
        this.eps = p.config.getIntProperty(EPS, DEFAULT_EPS);
        this.innerHandler = new InnerMessageHandler();
        this.latencyDetector = latDetector;
        this.localRTT = new double[n];
        this.rttMatrix = new double[n][n];
        this.listeners = new CopyOnWriteArrayList<LeaderOracleListener>();

        logger.info("Configuration: DELTA=" + delta + "; EPS=" + eps);
    }

    public LatencyDetector getLatencyDetector() {
        return latencyDetector;
    }

    public int getLeader() {
        return view % n;
    }

    public boolean isLeader() {
        return getLeader() == p.localID;
    }

    public void onNewRTTVector(double[] rttVector) {
        localRTT = rttVector;
    }

    public void registerLeaderOracleListener(LeaderOracleListener listener) {
        listeners.addIfAbsent(listener);
    }

    public void removeLeaderOracleListener(LeaderOracleListener listener) {
        listeners.remove(listener);
    }

    public void start() throws Exception {
        executor.executeAndWait(new Runnable() {
            public void run() {
                onStart();
            }
        });
    }

    public void stop() throws Exception {
        executor.executeAndWait(new Runnable() {
            public void run() {
                onStop();
            }
        });

    }

    // Reset the timer, cancel and reschedule task later
    // argument startTime correspond to the starting time of the timer
    @SuppressWarnings("unchecked")
    private void resetTimer(int startTime) {
        executor.checkInDispatcher();
        if (selectSendTask != null) {
            selectSendTask.cancel(false);
        }
        if (suspectTask != null) {
            suspectTask.cancel(false);
        }

        selectSendTask = (ScheduledFuture<SelectAndSendTask>) executor.schedule(
                new SelectAndSendTask(), 2 * delta - startTime, TimeUnit.MILLISECONDS);
        suspectTask = (ScheduledFuture<SuspectLeaderTask>) executor.schedule(
                new SuspectLeaderTask(), 3 * delta - startTime, TimeUnit.MILLISECONDS);
    }

    private int selectLeader() {
        executor.checkInDispatcher();
        assert isLeader();

        // Use the latest local RTT. The rttMatrix is cleared out later,
        // so make a deep copy of localRTT, as the vector should survive
        // the resetting of the matrix.
        rttMatrix[p.localID] = Arrays.copyOf(localRTT, localRTT.length);

        // Clone the rttMatrix and sort each vector
        double[][] tmpRttMatrix = new double[n][n];
        System.out.println(Util.toString(rttMatrix));
        for (int i = 0; i < n; i++) {
            tmpRttMatrix[i] = Arrays.copyOf(rttMatrix[i], n);
            Arrays.sort(tmpRttMatrix[i]);
        }
        System.out.println(Util.toString(tmpRttMatrix));

        int majIndex = n / 2; // (N/2 + 1) - 1 because index start at 0
        double currRtt = tmpRttMatrix[p.localID][majIndex];

        double minMajRTT = tmpRttMatrix[0][majIndex];
        int ii = 0;
        // Create a vector of the majRtt
        // Each pair contains the processId (key) and the majRtt (value)
        for (int i = 1; i < n; i++) {
            if (tmpRttMatrix[i][majIndex] < minMajRTT) {
                minMajRTT = tmpRttMatrix[i][majIndex];
                ii = i;
            }
        }

        logger.info("Leader majRTT: " + currRtt + ", minMaj: " + minMajRTT);

        // if(minMajRTT < (currRtt - 4*eps)) {
        if (minMajRTT < (currRtt - eps)) {
            StringBuffer sb = new StringBuffer("Select better leader: " + ii + " [");
            for (int i = 0; i < rttMatrix.length; i++) {
                sb.append(Util.toString(rttMatrix[i]));
                if (i < rttMatrix.length - 1) {
                    sb.append("; ");
                }
            }
            sb.append("]");
            logger.severe(sb.toString());
            return ii;
        } else {
            return getLeader();
        }
    }

    private void sendAlives() {
        executor.checkInDispatcher();

        for (int i = 0; i < n; i++) {
            Arrays.fill(rttMatrix[i], Double.MAX_VALUE);
        }

        resetTimer(0);

        SimpleAlive aliveMsg = new SimpleAlive(view);
        // Destination all except me
        BitSet destination = new BitSet(n);
        destination.set(0, n);
        destination.clear(p.localID);
        network.sendMessage(aliveMsg, destination);
    }

    private void startRound(int round, boolean sendStart) {
        startRound(round, sendStart, delta);
    }

    // Start round with a defined value for reseting the timer
    private void startRound(int round, boolean sendStart, int rstTimerVal) {
        executor.checkInDispatcher();

        view = round;
        int leader = view % n;

        // inform every listener of the change of leader
        for (LeaderOracleListener loListener : listeners) {
            loListener.onNewLeaderElected(leader);
        }
        logger.info("New view: " + round + " leader: " + leader);

        resetTimer(rstTimerVal);

        if (isLeader()) {
            logger.fine("I'm leader now.");
            sendAlives();
        } else {
            if (sendStart) {
                // Wake up the process that should become the leader
                Start startMsg = new Start(view);
                network.sendMessage(startMsg, leader);
            }
        }
    }

    void onAliveMessage(SimpleAlive msg, int sender) {
        executor.checkInDispatcher();
        int msgRound = msg.getView();
        if (msgRound < view) {
            network.sendMessage(new Start(view), sender);

        } else if (msgRound >= view) {
            if (msgRound == view) {
                resetTimer(0);
            } else {
                // don't send a start message because the leader is already
                // on the highest round
                startRound(msgRound, false, 0);
            }
            // Always send REPORT
            network.sendMessage(new Report(view, Arrays.copyOf(localRTT, localRTT.length)),
                    getLeader());
        }
    }

    void onReportMessage(Report msg, int sender) {
        executor.checkInDispatcher();

        // _logger.info("Received " + msg + " from " + sender);
        if (msg.getView() >= view) {
            assert msg.getView() == view : "Wrong view on REPORT message. Current view: " + view +
                                           ", msg: " + msg;
            rttMatrix[sender] = msg.getRTT();
        }
    }

    void onStartMessage(Start msg, int sender) {
        if (msg.getView() > view) {
            // No need to send a start message as we are the leader
            startRound(msg.getView(), false);
            assert isLeader() : "I'm not leader for round of START message: " + msg;
        }
    }

    private void onStart() {
        executor.checkInDispatcher();
        logger.info("Leader oracle starting");

        // Register interest in receiving network messages
        Network.addMessageListener(MessageType.SimpleAlive, innerHandler);
        Network.addMessageListener(MessageType.Report, innerHandler);
        Network.addMessageListener(MessageType.Start, innerHandler);

        if (view != -1) {
            throw new RuntimeException("Already started");
        }

        Arrays.fill(localRTT, Double.MAX_VALUE);
        view = 0;
        latencyDetector.registerLatencyDetectorListener(this);
        startRound(view, true);
        logger.info("Leader oracle started");
    }

    private void onStop() {
        executor.checkInDispatcher();
        logger.info("Leader oracle stopping");
        // remove the process from the message listener.
        Network.removeMessageListener(MessageType.SimpleAlive, innerHandler);
        Network.removeMessageListener(MessageType.Report, innerHandler);
        Network.removeMessageListener(MessageType.Start, innerHandler);

        if (selectSendTask != null) {
            selectSendTask.cancel(true);
            selectSendTask = null;
        }
        if (suspectTask != null) {
            suspectTask.cancel(true);
            suspectTask = null;
        }

        latencyDetector.removeLatencyDetectorListener(this);

        view = -1;
        logger.info("Leader oracle stopped");
    }

    private final class InnerMessageHandler extends MessageHandlerAdapter {
        public void onMessageReceived(final Message msg, final int sender) {
            // Execute on the dispatcher thread.
            executor.execute(new Handler() {
                public void handle() {
                    switch (msg.getType()) {
                        case SimpleAlive:
                            onAliveMessage((SimpleAlive) msg, sender);
                            break;

                        case Report:
                            onReportMessage((Report) msg, sender);
                            break;

                        case Start:
                            onStartMessage((Start) msg, sender);
                            break;

                        default:
                            logger.severe("Wrong message type received!!!");
                            System.exit(1);
                            break;
                    }

                }
            });
        }
    }

    final class SelectAndSendTask implements Runnable {
        public void run() {
            executor.checkInDispatcher();

            if (isLeader()) {
                int newLeader = selectLeader();

                if (newLeader == getLeader()) {
                    sendAlives();
                } else {
                    int nextRound = view;
                    while (nextRound % n != newLeader) {
                        nextRound++;
                    }

                    // Send START to force the new leader to advance
                    startRound(nextRound, true);
                }
            }
        }
    }

    final class SuspectLeaderTask implements Runnable {
        public void run() {
            if (!isLeader()) {
                logger.info("Suspecting leader: " + getLeader());
                startRound(view + 1, true);
            }
        }
    }

    public int getEpsilon() {
        return eps;
    }

    public int getDelta() {
        return delta;
    }
}
