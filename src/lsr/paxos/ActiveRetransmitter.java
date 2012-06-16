package lsr.paxos;

import java.util.BitSet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.MovingAverage;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;
import lsr.paxos.statistics.ReplicaStats;

/**
 * Manages retransmissions of messages using a dedicated thread and a delay
 * queue.
 * 
 * When {@link #startTransmitting(Message)} is called, this class sends the
 * message using the calling thread (should be the Dispatcher) then computes the
 * time for the next retransmission, and enqueues the message in an internal
 * instance of {@link DelayQueue} to expire at the time of retransmission. The
 * internal thread blocks on the queue, retransmits messages whose delay expired
 * and re-enqueues for further retransmission.
 */
public final class ActiveRetransmitter implements Runnable, Retransmitter {
    private final static BitSet ALL_BUT_ME = ALL_BUT_ME_initializer();

    private static BitSet ALL_BUT_ME_initializer() {
        BitSet bs = new BitSet(ProcessDescriptor.getInstance().numReplicas);
        bs.set(0, ProcessDescriptor.getInstance().numReplicas);
        bs.clear(ProcessDescriptor.getInstance().localId);
        return bs;
    }

    private final Network network;
    private final DelayQueue<InnerRetransmittedMessage> queue =
            new DelayQueue<ActiveRetransmitter.InnerRetransmittedMessage>();

    private final static MovingAverage ma = new MovingAverage(0.1,
            ProcessDescriptor.getInstance().retransmitTimeout);
    private final String name;

    /**
     * See {@link #ActiveRetransmitter(Network network, String name)}.
     */
    public ActiveRetransmitter(Network network) {
        this(network, "AnonymousRetransmitter");
    }

    /**
     * Initializes new instance of retransmitter.
     * 
     * @param network - the network used to send messages to other replicas
     * @param name - a name for the thread handling (re)transmission
     */
    public ActiveRetransmitter(Network network, String name) {
        assert network != null;
        this.network = network;
        this.name = name;
    }

    public void init() {
        thread = new Thread(this, name);
        thread.start();
    }

    /**
     * Starts retransmitting specified message to all processes except local
     * process. The message is sent immediately after calling this method, and
     * then retransmitted at fixed-rate.
     * 
     * @param message - the message to retransmit
     * @return the handler used to control retransmitting message
     */
    public RetransmittedMessage startTransmitting(Message message) {
        // destination gets cloned in the InnerRetransmittedMessage, so using
        // here a constant is correct
        return startTransmitting(message, ALL_BUT_ME);
    }

    public RetransmittedMessage startTransmitting(Message message, BitSet destinations) {
        return startTransmitting(message, destinations, -1);
    }

    /**
     * Starts retransmitting specified message to processes specified in
     * destination parameter. The message is sent immediately after calling this
     * method, and then retransmitted at fixed-rate.
     * 
     * @param message - the message to retransmit
     * @param destinations - bit set containing list of replicas to which
     *            message should be retransmitted
     * @return the handler used to control retransmitting message
     */
    public RetransmittedMessage startTransmitting(Message message, BitSet destinations, int cid) {
        InnerRetransmittedMessage handler = new InnerRetransmittedMessage(message, destinations,
                cid);
        // First attempt is done directly by the dispatcher thread. Therefore,
        // in the normal
        // case, there is no additional context switch to send a message.
        // retransmit() will enqueue the message for additional retransmission.
        handler.retransmit();
        return handler;
    }

    /**
     * Stops retransmitting all messages.
     */
    public void stopAll() {
        queue.clear();
    }

    @Override
    public void run() {
        logger.info("ActiveRetransmitter starting");
        try {
            while (!Thread.interrupted()) {
                // The message might be canceled between take() returns and
                // retrasmit() is called.
                // To avoid retransmitting canceled messages, the
                // RetransMessage.stop() sets a
                // canceled flag to false, which is checked before
                // retransmission
                InnerRetransmittedMessage rMsg = queue.take();
                rMsg.retransmit();
            }
        } catch (InterruptedException e) {
            logger.warning("Thread dying: " + e.getMessage());
        }
    }

    /**
     * Thread safety: This class is accessed both by the Dispatcher
     * (retransmit(), stop() and start()) and by the Retransmitter thread
     * (getDelay(), compareTo() and retransmit())
     * 
     * @author Nuno Santos (LSR)
     */
    final class InnerRetransmittedMessage implements RetransmittedMessage, Delayed {
        private final Message message;
        private final BitSet destinations;

        /** Last retransmission time */
        private long sendTs = -1;
        /** The time the task is enabled to execute in milliseconds */
        private volatile long time = -1;
        private boolean cancelled = false;

        private final int cid;

        InnerRetransmittedMessage(Message message, BitSet destinations) {
            this(message, destinations, -1);
        }

        InnerRetransmittedMessage(Message message, BitSet destinations, int cid) {
            this.message = message;
            this.cid = cid;
            // the destination is cloned to not changing the original one while
            // stopping some destinations
            this.destinations = (BitSet) destinations.clone();
        }

        // -----------------------------------------
        // RetransmittedMessage interface implementation
        // -----------------------------------------
        public synchronized void stop(int destination) {
            this.destinations.clear(destination);
            if (this.destinations.isEmpty()) {
                stop();
            }
        }

        public synchronized void stop() {
            queue.remove(this);
            assert sendTs != 0;
            // Update moving average with how long it took
            // until this message stops being retransmitted
            ma.add(System.currentTimeMillis() - sendTs);
            cancelled = true;
        }

        public synchronized void start(int destination) {
            destinations.set(destination);
        }

        // -----------------------------------------
        // Delayed interface implementation
        // -----------------------------------------
        public long getDelay(TimeUnit unit) {
            // time is volatile, so there is no need for synchronization
            return unit.convert(time - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed other) {
            if (other == this) // compare zero ONLY if same object
                return 0;
            if (other instanceof InnerRetransmittedMessage) {
                InnerRetransmittedMessage x = (InnerRetransmittedMessage) other;
                long diff = time - x.time;
                if (diff < 0)
                    return -1;
                else if (diff > 0)
                    return 1;
                else if (this.hashCode() < other.hashCode())
                    return 1;
                else
                    return -1;
            }
            // Release the lock
            // TODO: JK: what lock?
            long d = (getDelay(TimeUnit.NANOSECONDS) -
                    other.getDelay(TimeUnit.NANOSECONDS));
            int res = (d == 0) ? 0 : ((d < 0) ? -1 : 1);
            logger.severe("Investigate!!! two different objects return 0 in compareTo function");
            return res;
        }

        // //////////////////////////////////////
        // ActiveRetransmitter scoped methods
        // //////////////////////////////////////
        synchronized void retransmit() {
            // Task might have been canceled since it was dequeued.
            if (cancelled) {
                logger.warning("Trying to retransmit a cancelled message");
                return;
            }
            if (cid != -1) {
                ReplicaStats.getInstance().retransmit(cid);
            }
            // Can be called either by Dispatcher (first time message is sent)
            // or by Retransmitter thread (retransmissions)
            sendTs = System.currentTimeMillis();
            // Due to the lock on "this", destinations does not change while
            // this method
            // is called.
            network.sendMessage(message, destinations);
            // Schedule the next attempt
            // Impose a lower bound on retransmission frequency to prevent
            // excessive retransmission
            time = sendTs + Math.max((int) (ma.get() * 3), 5000);
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Resending in: " + getDelay(TimeUnit.MILLISECONDS));
            }
            queue.offer(this);
        }
    }

    private final static Logger logger = Logger.getLogger(ActiveRetransmitter.class.getCanonicalName());
    private Thread thread;
}
