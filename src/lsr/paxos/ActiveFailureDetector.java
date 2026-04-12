package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import lsr.paxos.messages.Alive;
import lsr.paxos.messages.AliveReply;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents failure detector thread. If the current process is the leader,
 * then this class is responsible for sending <code>ALIVE</code> message every
 * amount of time. Otherwise is responsible for suspecting the leader. If there
 * is no message received from leader, then the leader is suspected to crash,
 * and <code>Paxos</code> is notified about this event.
 */
final public class ActiveFailureDetector implements Runnable, FailureDetector {

    /** How long to wait until suspecting the leader. In milliseconds */
    private volatile int suspectTimeout;
    /** How long the leader waits until sending heartbeats. In milliseconds */
    private volatile int sendTimeout;
    private final int defaultSuspectTimeout;
    private final int defaultSendTimeout;

    private final Network network;
    private final MessageHandler innerListener;
    private final Storage storage;
    private final Thread thread;

    private int view;

    /** Follower role: reception time of the last heartbeat from the leader */
    private volatile long lastHeartbeatRcvdTS;
    /** Leader role: time when the last message or heartbeat was sent to all */
    private volatile long lastHeartbeatSentTS;
    /** Leader role: monotonically increasing heartbeat id */
    private long nextHeartbeatId;
    /** Leader role: send timestamp of recent heartbeat ids */
    private final Map<Long, Long> heartbeatSendTsById = new HashMap<Long, Long>();
    /** Leader role: last RTT observed from each follower */
    private final Map<Integer, Long> lastRttByFollower = new HashMap<Integer, Long>();
    /** Leader role: last one-way delay estimate (RTT/2) for each follower */
    private final Map<Integer, Long> lastOneWayDelayByFollower = new HashMap<Integer, Long>();
    private long oldestTrackedHeartbeatId;
    private static final int MAX_TRACKED_HEARTBEATS = 4096;
    /** Follower role: observed RTT samples from leader heartbeats. */
    private final ArrayDeque<Long> observedRtts = new ArrayDeque<Long>();
    /** Follower role: observed heartbeat ids for loss estimation. */
    private final ArrayDeque<Long> observedHeartbeatIds = new ArrayDeque<Long>();
    /** Follower role: latest heartbeat id observed (monotonic filter). */
    private long lastObservedHeartbeatId = -1;
    /** Follower role: latest computed E_t (suspicion timeout) in milliseconds. */
    private long lastComputedEt = -1;
    /** Follower role: latest suggested heartbeat interval for leader. */
    private int lastSuggestedHeartbeatInterval = -1;

    private final FailureDetectorListener fdListener;

    /**
     * Initializes new instance of <code>FailureDetector</code>.
     * 
     * @param paxos - the paxos which should be notified about suspecting leader
     * @param network - used to send and receive messages
     * @param storage - storage containing all data about paxos
     */
    public ActiveFailureDetector(FailureDetectorListener fdListener, Network network,
                                 Storage storage) {
        this.fdListener = fdListener;
        this.network = network;
        this.storage = storage;
        defaultSuspectTimeout = processDescriptor.fdSuspectTimeout;
        defaultSendTimeout = processDescriptor.fdSendTimeout;
        suspectTimeout = defaultSuspectTimeout;
        sendTimeout = defaultSendTimeout;
        thread = new Thread(this, "FailureDetector");
        thread.setDaemon(true);
        innerListener = new InnerMessageHandler();
        storage.addViewChangeListener(viewCahngeListener);
    }

    public int getDefaultSuspectTimeout() {
        return defaultSuspectTimeout;
    }

    public int getDefaultSendTimeout() {
        return defaultSendTimeout;
    }

    public int getSuspectTimeout() {
        return suspectTimeout;
    }

    public int getSendTimeout() {
        return sendTimeout;
    }

    public void setSuspectTimeout(int suspectTimeout) {
        validateTimeout("suspectTimeout", suspectTimeout);
        synchronized (this) {
            this.suspectTimeout = suspectTimeout;
            notifyAll();
        }
    }

    public void setSendTimeout(int sendTimeout) {
        validateTimeout("sendTimeout", sendTimeout);
        synchronized (this) {
            this.sendTimeout = sendTimeout;
            notifyAll();
        }
    }

    public void restoreDefaultTimeouts() {
        synchronized (this) {
            suspectTimeout = defaultSuspectTimeout;
            sendTimeout = defaultSendTimeout;
            notifyAll();
        }
    }

    public long getLastRttForReplica(int replicaId) {
        synchronized (this) {
            Long rtt = lastRttByFollower.get(replicaId);
            return rtt == null ? -1 : rtt.longValue();
        }
    }

    public long getLastOneWayDelayForReplica(int replicaId) {
        synchronized (this) {
            Long oneWayDelay = lastOneWayDelayByFollower.get(replicaId);
            return oneWayDelay == null ? -1 : oneWayDelay.longValue();
        }
    }

    public int getObservedRttCount() {
        synchronized (this) {
            return observedRtts.size();
        }
    }

    public int getObservedHeartbeatIdCount() {
        synchronized (this) {
            return observedHeartbeatIds.size();
        }
    }

    public long getLastObservedRtt() {
        synchronized (this) {
            Long last = observedRtts.peekLast();
            return last == null ? -1 : last.longValue();
        }
    }

    public long getLastComputedEt() {
        synchronized (this) {
            return lastComputedEt;
        }
    }

    public int getLastSuggestedHeartbeatInterval() {
        synchronized (this) {
            return lastSuggestedHeartbeatInterval;
        }
    }

    /**
     * Starts failure detector.
     */
    public void start(int initialView) {
        synchronized (this) {
            view = initialView;
            thread.start();
        }
        // Any message received from the leader serves also as an ALIVE message.
        Network.addMessageListener(MessageType.ANY, innerListener);
        // Sent messages used when in leader role: also count as ALIVE message
        // so don't reset sending timeout.
        Network.addMessageListener(MessageType.SENT, innerListener);
    }

    /**
     * Stops failure detector.
     */
    public void stop() {
        Network.removeMessageListener(MessageType.ANY, innerListener);
        Network.removeMessageListener(MessageType.SENT, innerListener);
    }

    /**
     * Updates state of failure detector, due to leader change.
     * 
     * Called whenever the leader changes.
     * 
     * @param newLeader - process id of the new leader
     */
    protected Storage.ViewChangeListener viewCahngeListener = new Storage.ViewChangeListener() {

        public void viewChanged(int newView, int newLeader) {
            synchronized (ActiveFailureDetector.this) {
                logger.debug("FD has been informed about view {}", newView);
                view = newView;
                lastHeartbeatRcvdTS = getTime();
                resetFollowerObservations();
                ActiveFailureDetector.this.notifyAll();
            }
        }
    };

    public void run() {
        logger.info("Starting failure detector");
        try {
            // Warning for maintainers: Deadlock danger!!
            // The code below calls several methods in other classes while
            // holding the this lock.
            // If the methods called acquire locks and then try to call into
            // this failure detector,
            // there is the danger of deadlock. Therefore, always ensure that
            // the methods called
            // below do not themselves obtain locks.
            synchronized (this) {
                while (true) {
                    long now = getTime();
                    // Leader role
                    if (processDescriptor.isLocalProcessLeader(view)) {
                        // Send
                        long heartbeatId = nextHeartbeatId++;
                        long rttToEmbed = -1;
                        long heartbeatIntervalToEmbed = sendTimeout;
                        Alive alive = new Alive(view, storage.getLog().getNextId(),
                                heartbeatId, rttToEmbed, heartbeatIntervalToEmbed);
                        trackHeartbeatSendTime(heartbeatId, now);
                        network.sendToOthers(alive);
                        lastHeartbeatSentTS = now;
                        long nextSend = lastHeartbeatSentTS + sendTimeout;

                        while (now < nextSend && processDescriptor.isLocalProcessLeader(view)) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Sending next Alive in {} ms", nextSend - now);
                            }
                            wait(nextSend - now);
                            // recompute the state. lastHBSentTS might have
                            // changed.
                            now = getTime();
                            nextSend = lastHeartbeatSentTS + sendTimeout;
                        }
                        // Either no longer the leader or the it is time to send
                        // an hearbeat

                    } else {
                        // follower role
                        lastHeartbeatRcvdTS = now;
                        long suspectTime = lastHeartbeatRcvdTS + suspectTimeout;
                        // Loop until either this process becomes the leader or
                        // until is time to suspect the leader
                        while (now < suspectTime && !processDescriptor.isLocalProcessLeader(view)) {

                            if (logger.isTraceEnabled()) {
                                logger.trace("Suspecting leader ({}) in {} ms",
                                        processDescriptor.getLeaderOfView(view), suspectTime - now);
                            }

                            wait(suspectTime - now);
                            now = getTime();
                            suspectTime = lastHeartbeatRcvdTS + suspectTimeout;
                        }
                        if (!processDescriptor.isLocalProcessLeader(view)) {
                            // Raise the suspicion. A suspect task will be
                            // queued for execution
                            // on the Protocol thread.
                            fdListener.suspect(view);
                            // The view change is done asynchronously as seen
                            // from this thread.
                            // To avoid raising multiple suspicions, this thread
                            // suspends until
                            // the view change completes. When that happens, the
                            // method viewChange()
                            // will be called by the Protocol thread, which will
                            // notify() this
                            // monitor, thereby unlocking this thread.
                            int oldView = view;
                            while (oldView == view) {
                                logger.debug("FD is waiting for view change from {}", oldView);
                                wait();
                            }
                            logger.debug("FD now knows about new view");
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Intersects any message sent or received, used to reset the timeouts for
     * sending and receiving ALIVE messages.
     * 
     * These methods are called by the Network thread.
     * 
     * @author Nuno Santos (LSR)
     */
    final class InnerMessageHandler implements MessageHandler {

        public void onMessageReceived(Message message, int sender) {
            if (processDescriptor.isLocalProcessLeader(view)) {
                if (message.getType() == MessageType.AliveReply) {
                    handleAliveReply((AliveReply) message, sender);
                }
                return;
            }

            // Use the message as heartbeat if the local process is
            // a follower and the sender is the leader of the current view
            if (sender == processDescriptor.getLeaderOfView(view)) {
                if (message.getType() == MessageType.Alive) {
                    Alive alive = (Alive) message;
                    if (alive.getView() != view) {
                        return;
                    }
                    lastHeartbeatRcvdTS = getTime();
                    observeFollowerHeartbeat(alive);
                    if (alive.getHeartbeatId() >= 0) {
                        long calculatedHeartbeatInterval = getSuggestedHeartbeatIntervalForReply();
                        network.sendMessage(
                                new AliveReply(alive.getView(), alive.getHeartbeatId(),
                                        calculatedHeartbeatInterval),
                                sender);
                    }
                } else {
                    lastHeartbeatRcvdTS = getTime();
                }
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
            // leader only.
            if (!processDescriptor.isLocalProcessLeader(view))
                return;

            // Ignore Alive messages, the clock was already reset when the
            // message was sent.
            if (message.getType() == MessageType.Alive) {
                return;
            }
            // If the message is not sent to all, ignore it as it is not useful
            // as an hearbeat. Use n-1 because a process does not send to self
            if (destinations.cardinality() < processDescriptor.numReplicas - 1) {
                return;
            }

            // Check if comment above is true
            assert !destinations.get(processDescriptor.localId) : message;

            // This process just sent a message to all. Reset the timeout.
            lastHeartbeatSentTS = getTime();
        }
    }

    static long getTime() {
        // return System.currentTimeMillis();
        return System.nanoTime() / 1000000;
    }

    private static void validateTimeout(String timeoutName, int timeoutValue) {
        if (timeoutValue <= 0) {
            throw new IllegalArgumentException(timeoutName + " must be positive.");
        }
    }

    private void trackHeartbeatSendTime(long heartbeatId, long sentTs) {
        synchronized (this) {
            heartbeatSendTsById.put(heartbeatId, sentTs);
            while (heartbeatSendTsById.size() > MAX_TRACKED_HEARTBEATS) {
                heartbeatSendTsById.remove(oldestTrackedHeartbeatId);
                oldestTrackedHeartbeatId++;
            }
        }
    }

    private void handleAliveReply(AliveReply reply, int sender) {
        synchronized (this) {
            if (reply.getView() != view) {
                return;
            }
            Long sentTs = heartbeatSendTsById.get(reply.getHeartbeatId());
            if (sentTs == null) {
                return;
            }
            long now = getTime();
            long rtt = now - sentTs.longValue();
            if (rtt < 0) {
                return;
            }
            lastRttByFollower.put(sender, rtt);
            lastOneWayDelayByFollower.put(sender, rtt / 2);

            long heartbeatInterval = reply.getHeartbeatInterval();
            if (heartbeatInterval <= 0 || heartbeatInterval > Integer.MAX_VALUE) {
                logger.warn("Ignoring invalid heartbeat interval {} from replica {}",
                        heartbeatInterval, sender);
                return;
            }

            int newSendTimeout = (int) heartbeatInterval;
            if (newSendTimeout != sendTimeout) {
                logger.debug(
                        "Adjusting sendTimeout from {} to {} based on feedback from replica {}",
                        sendTimeout, newSendTimeout, sender);
                setSendTimeout(newSendTimeout);
            }
        }
    }

    private void observeFollowerHeartbeat(Alive alive) {
        synchronized (this) {
            if (alive.getRtt() >= 0) {
                observedRtts.addLast(alive.getRtt());
                trimWindow(observedRtts);
            }
            if (alive.getHeartbeatId() >= 0) {
                long heartbeatId = alive.getHeartbeatId();
                if (heartbeatId > lastObservedHeartbeatId) {
                    lastObservedHeartbeatId = heartbeatId;
                    observedHeartbeatIds.addLast(heartbeatId);
                    trimWindow(observedHeartbeatIds);
                }
            }
            updateFollowerTuning();
        }
    }

    private void resetFollowerObservations() {
        observedRtts.clear();
        observedHeartbeatIds.clear();
        lastComputedEt = -1;
        lastSuggestedHeartbeatInterval = -1;
        lastObservedHeartbeatId = -1;
    }

    private static <T> void trimWindow(ArrayDeque<T> window) {
        while (window.size() > processDescriptor.dynatuneMaxListSize) {
            window.removeFirst();
        }
    }

    private long getSuggestedHeartbeatIntervalForReply() {
        synchronized (this) {
            if (lastSuggestedHeartbeatInterval > 0) {
                return lastSuggestedHeartbeatInterval;
            }
            return Math.max(1, suspectTimeout / 2);
        }
    }

    private void updateFollowerTuning() {
        if (!processDescriptor.dynatuneEnabled) {
            return;
        }
        int minListSize = processDescriptor.dynatuneMinListSize;
        if (observedRtts.size() < minListSize ||
            observedHeartbeatIds.size() < minListSize) {
            return;
        }
        double mean = computeMean(observedRtts);
        double stddev = computeStdDev(observedRtts, mean);
        double et = mean + processDescriptor.dynatuneSafetyFactor * stddev;
        int newSuspectTimeout = clampToPositiveIntCeil(et);
        if (newSuspectTimeout > 0 && newSuspectTimeout != suspectTimeout) {
            setSuspectTimeout(newSuspectTimeout);
        }
        lastComputedEt = newSuspectTimeout;

        double packetLossRate = computePacketLossRate(observedHeartbeatIds);
        int suggestedInterval = computeSuggestedHeartbeatInterval(newSuspectTimeout, packetLossRate,
                processDescriptor.dynatuneHeartbeatProbability);
        if (suggestedInterval > 0) {
            lastSuggestedHeartbeatInterval = suggestedInterval;
        }
    }

    private static double computeMean(ArrayDeque<Long> samples) {
        long sum = 0;
        for (Long sample : samples) {
            sum += sample.longValue();
        }
        return sum / (double) samples.size();
    }

    private static double computeStdDev(ArrayDeque<Long> samples, double mean) {
        if (samples.size() <= 1) {
            return 0.0;
        }
        double variance = 0.0;
        for (Long sample : samples) {
            double delta = sample.longValue() - mean;
            variance += delta * delta;
        }
        variance /= samples.size();
        return Math.sqrt(variance);
    }

    private static double computePacketLossRate(ArrayDeque<Long> heartbeatIds) {
        if (heartbeatIds.size() < 2) {
            return 0.0;
        }
        long first = heartbeatIds.peekFirst().longValue();
        long last = heartbeatIds.peekLast().longValue();
        long expected = calculatePacketCount(first, last);
        long received = heartbeatIds.size();
        if (expected <= 0) {
            return 0.0;
        }
        double packetLossRate = 1.0 - (received / (double) expected);
        if (packetLossRate < 0.0) {
            return 0.0;
        }
        if (packetLossRate > 1.0) {
            return 1.0;
        }
        return packetLossRate;
    }

    private static int computeSuggestedHeartbeatInterval(double et, double packetLossRate,
                                                         double targetProbability) {
        if (et <= 0) {
            return -1;
        }
        double ceilLogTerm;
        if (packetLossRate <= 0.0) {
            ceilLogTerm = 1.0;
        } else if (packetLossRate >= 1.0) {
            return -1;
        } else {
            double logTerm = Math.log(1.0 - targetProbability) / Math.log(packetLossRate) + 1.0;
            ceilLogTerm = Math.ceil(logTerm);
        }
        double interval = Math.floor(et / (ceilLogTerm + 1.0));
        return clampToPositiveIntCeil(interval);
    }

    private static int clampToPositiveIntCeil(double value) {
        if (value <= 0) {
            return -1;
        }
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) Math.ceil(value);
    }

    private static long calculatePacketCount(long firstId, long lastId) {
        if (lastId >= firstId) {
            return lastId - firstId + 1;
        }
        return (Long.MAX_VALUE - firstId) + lastId + 2;
    }

    private final static Logger logger = LoggerFactory.getLogger(ActiveFailureDetector.class);
}
