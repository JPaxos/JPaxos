package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Storage;

/**
 * Represents failure detector thread. If the current process is the leader,
 * then this class is responsible for sending <code>ALIVE</code> message every
 * amount of time. Otherwise is responsible for suspecting the leader. If there
 * is no message received from leader, then the leader is suspected to crash,
 * and <code>Paxos</code> is notified about this event.
 */
final public class ActiveFailureDetector implements Runnable, FailureDetector {
    /** How long to wait until suspecting the leader. In milliseconds */
    private final int suspectTimeout;
    /** How long the leader waits until sending heartbeats. In milliseconds */
    private final int sendTimeout;

    private final Network network;
    private final MessageHandler innerListener;
    private final Storage storage;
    private final Thread thread;

    private int view;
    /** Follower role: reception time of the last heartbeat from the leader */
    private volatile long lastHeartbeatRcvdTS;
    /** Leader role: time when the last message or heartbeat was sent to all */
    private volatile long lastHeartbeatSentTS;

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
        this.suspectTimeout = processDescriptor.fdSuspectTimeout;
        this.sendTimeout = processDescriptor.fdSendTimeout;
        this.thread = new Thread(this, "FailureDetector");
        this.innerListener = new InnerMessageHandler();
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
    public void viewChange(int newView) {
        synchronized (this) {
            view = newView;
            notify();
        }
    }

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
                        Alive alive = new Alive(view, storage.getLog().getNextId());
                        network.sendToAllButMe(alive);
                        lastHeartbeatSentTS = now;
                        long nextSend = lastHeartbeatSentTS + sendTimeout;

                        while (now < nextSend && processDescriptor.isLocalProcessLeader(view)) {
                            if (logger.isLoggable(Level.FINE)) {
                                logger.fine("Sending next Alive in " + (nextSend - now) + "ms");
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
                            if (logger.isLoggable(Level.FINE)) {
                                logger.fine("Suspecting leader (" +
                                            processDescriptor.getLeaderOfView(view) +
                                            ") in " + (suspectTime - now) + "ms");
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
                                wait();
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            logger.warning("Thread dying: " + ex.getMessage());
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
            if (!processDescriptor.isLocalProcessLeader(view) &&
                sender == processDescriptor.getLeaderOfView(view)) {
                lastHeartbeatRcvdTS = getTime();
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
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
            // This process just sent a message to all.
            // If leader, update the lastHeartbeatSentTS
            if (processDescriptor.isLocalProcessLeader(view)) {
                lastHeartbeatSentTS = getTime();
            }
        }
    }

    static long getTime() {
        // TODO: JK: out of pure interests: why nanotime?
        // return System.currentTimeMillis();
        return System.nanoTime() / 1000000;
    }

    private final static Logger logger =
            Logger.getLogger(ActiveFailureDetector.class.getCanonicalName());
}
