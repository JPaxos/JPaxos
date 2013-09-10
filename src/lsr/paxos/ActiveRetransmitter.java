package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import lsr.common.MovingAverage;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final Network network;
    private final String name;

    private Thread thread;

    private final DelayQueue<InnerRetransmittedMessage> queue =
            new DelayQueue<ActiveRetransmitter.InnerRetransmittedMessage>();
    private final static MovingAverage ma = new MovingAverage(0.1,
            processDescriptor.retransmitTimeout);

    /**
     * Initializes new instance of retransmitter.
     * 
     * @param network - the network used to send messages to other replicas
     */
    public ActiveRetransmitter(Network network, String name) {
        assert network != null;
        this.network = network;
        this.name = name;
    }

    @Override
    public void init() {
        thread = new Thread(this, name);
        thread.start();
    }

    @Override
    public void close() {
        stopAll();
        thread.interrupt();
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
        // no need to clone ALL_BUT_ME - the constructor down there does this
        return startTransmitting(message, Network.OTHERS);
    }

    /**
     * Starts retransmitting specified message to processes specified in
     * destination parameter. The message is sent immediately after calling this
     * method, and then retransmitted at fixed-rate.
     * 
     * @param message - the message to retransmit
     * @param destinations - bit set containing list of replicas to which
     *            message should be retransmitted. Destinations is cloned inside
     *            this method.
     * @return the handler used to control retransmitting message
     */
    public RetransmittedMessage startTransmitting(Message message, BitSet destinations) {
        InnerRetransmittedMessage handler = new InnerRetransmittedMessage(message, destinations);
        /*
         * First attempt is done directly by the dispatcher thread. Therefore,
         * in the normal case, there is no additional context switch to send a
         * message. retransmit() will enqueue the message for additional
         * retransmission.
         */
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
        logger.info("ActiveRetransmitter '{}' starting", name);
        try {
            while (!Thread.interrupted()) {
                /*
                 * The message might be canceled between take() returns and
                 * retrasmit() is called. To avoid retransmitting canceled
                 * messages, the RetransMessage.stop() sets a canceled flag to
                 * false, which is checked before retransmission
                 */
                InnerRetransmittedMessage rMsg = queue.take();
                rMsg.retransmit();
            }
        } catch (InterruptedException e) {
            logger.warn("ActiveRetransmitter '{}' closing: {}", name, e);
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
        private static final int MIN_RETRANSMIT_TIME = 200;
        private final Message message;
        private final BitSet destinations;

        /** Last retransmission time */
        private long sendTs = -1;
        /** The time the task is enabled to execute in milliseconds */
        private volatile long time = -1;
        private boolean cancelled = false;

        InnerRetransmittedMessage(Message message, BitSet destinations) {
            this.message = message;
            // the destination is cloned to not changing the original one while
            // stopping some destinations
            this.destinations = (BitSet) destinations.clone();
        }

        // -----------------------------------------
        // RetransmittedMessage interface implementation
        // -----------------------------------------
        public synchronized void start(int destination) {
            destinations.set(destination);
        }

        public synchronized void stop(int destination) {
            this.destinations.clear(destination);
            if (this.destinations.isEmpty()) {
                stop();
            }
        }

        public synchronized void stop() {
            queue.remove(this);
            assert sendTs != -1;
            // Update moving average with how long it took
            // until this message stops being retransmitted
            ma.add(System.currentTimeMillis() - sendTs);
            cancelled = true;
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
                else
                    return 1;
            }

            long d = (getDelay(TimeUnit.NANOSECONDS) -
                    other.getDelay(TimeUnit.NANOSECONDS));
            return (d < 0) ? -1 : 1;
        }

        // //////////////////////////////////////
        // ActiveRetransmitter scoped methods
        // //////////////////////////////////////
        synchronized void retransmit() {
            // Can be called either by Dispatcher (first time message is sent)
            // or by Retransmitter thread (retransmissions)

            // Task might have been canceled since it was dequeued.
            if (cancelled) {
                logger.error("Trying to retransmit a cancelled message");
                return;
            }
            sendTs = System.currentTimeMillis();
            network.sendMessage(message, destinations);
            // Schedule the next attempt
            time = sendTs + Math.max((int) (ma.get() * 3), MIN_RETRANSMIT_TIME);
            if (logger.isTraceEnabled()) {
                logger.trace("Resending in: {}", getDelay(TimeUnit.MILLISECONDS));
            }
            queue.offer(this);
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(ActiveRetransmitter.class);
}
