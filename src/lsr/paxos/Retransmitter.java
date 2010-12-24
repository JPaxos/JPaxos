package lsr.paxos;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Config;
import lsr.common.Dispatcher;
import lsr.common.DispatcherImpl.Priority;
import lsr.common.MovingAverage;
import lsr.common.PriorityTask;
import lsr.paxos.messages.Message;
import lsr.paxos.network.Network;

/**
 * Implementation of simple {@link Retransmitter} based on {@link Timer}. When
 * there are no messages to retransmit then the timer is stopped. After starting
 * retransmitting first message the new timer task is created and the message is
 * added to list of retransmitting messages.
 * 
 */
public class Retransmitter {
    private final Network network;
    private final Dispatcher dispatcher;
    private final int nProcesses;

    private final Map<InnerRetransmittedMessage, PriorityTask> messages = new HashMap<InnerRetransmittedMessage, PriorityTask>();

    // /* Stores the messages that are ready to be retransmitted
    // * until there are no incoming messages queued for processing
    // */
    // private final Queue<InnerRetransmittedMessage> readyMsgs =
    // new ArrayDeque<InnerRetransmittedMessage>(32);

    private final MovingAverage ma = new MovingAverage(0.1, Config.RETRANSMIT_TIMEOUT);

    /**
     * Initializes new instance of retransmitter.
     * 
     * @param network - the network used to send messages to other replicas
     * @param nProcesses - the number of processes(replicas)
     */
    public Retransmitter(Network network, int nProcesses, Dispatcher dispatcher) {
        assert network != null;
        this.dispatcher = dispatcher;
        this.network = network;
        this.nProcesses = nProcesses;
    }

    /**
     * Starts retransmitting specified message to all processes. The message is
     * sent immediately after calling this method, and then retransmitted at
     * fixed-rate.
     * 
     * @param message - the message to retransmit
     * @return the handler used to control retransmitting message
     */
    public RetransmittedMessage startTransmitting(Message message) {
        BitSet bs = new BitSet(nProcesses);
        bs.set(0, nProcesses);
        return startTransmitting(message, bs);
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
    public RetransmittedMessage startTransmitting(Message message, BitSet destinations) {
        InnerRetransmittedMessage handler = new InnerRetransmittedMessage(message, destinations);
        // First attempt
        handler.retransmit();
        return handler;
    }

    /**
     * Stops retransmitting all messages.
     */
    public void stopAll() {
        assert dispatcher.amIInDispatcher();
        /*
         * Make a copy of the keys. Otherwise, using the _messages iterator will
         * result in ConcurrentModificationException because the stop() method
         * removes the element from the _messages array.
         */
        InnerRetransmittedMessage[] array = messages.keySet().toArray(
                new InnerRetransmittedMessage[messages.size()]);
        for (int i = 0; i < array.length; i++) {
            array[i].stop();
        }
        messages.clear();
        // readyMsgs.clear();
    }

    long getRttEstimate() {
        // Use a value slightly higher than ma estimate to
        // have some tolerance to the natural variance.
        // TODO: A more scientific solution: compute the standard
        // deviation and use a multiplier factor such that
        // X% (eg, 95%) of all RTT are within the estimated RTT
        // The minimum retransmission bound is to prevent
        // overloading the CPU during busy periods.

        // Conservative estimate
        return Math.max(10, (long) (ma.get() * 7));
        // Fixed estimate
        // return 100;
    }

    class InnerRetransmittedMessage implements RetransmittedMessage, Runnable {
        private final Message message;
        private final BitSet destination;
        private final long sendTs;

        public InnerRetransmittedMessage(Message message, BitSet destination) {
            this.message = message;
            // the destination is cloned to not changing the original one while
            // stopping some destinations
            this.destination = (BitSet) destination.clone();
            sendTs = System.currentTimeMillis();
        }

        public void stop(int destination) {
            this.destination.clear(destination);
            if (this.destination.isEmpty())
                stop();
        }

        public void stop() {
            PriorityTask pTask = messages.remove(this);
            if (pTask == null) {
                logger.warning("Task already canceled: " + pTask);
            } else {
                pTask.cancel();
            }

            // Update moving average with how long it took
            // until this message stops being retransmitted
            ma.add(System.currentTimeMillis() - sendTs);
        }

        public Message getMessage() {
            return message;
        }

        public BitSet getDestination() {
            return destination;
        }

        public void run() {
            // assert !readyMsgs.contains(this) : "Message already queued: " +
            // _message;
            // if (_dispatcher.getQueuedIncomingMsgs() != 0) {
            // if (_logger.isLoggable(Level.INFO)) {
            // _logger.info("Delaying retransmission " + _message +
            // ", queued messages: " + _dispatcher.getQueuedIncomingMsgs());
            // }
            // // There are incoming messages waiting to be processed.
            // // Do not retransmit, wait until the input queue is empty
            // readyMsgs.add(this);
            //
            // } else {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Retransmitting " + message + " to " + destination);
            }

            // System is idle, retransmit immediately
            retransmit();
            // }
        }

        public void retransmit() {
            network.sendMessage(message, destination);
            // Schedule the next attempt
            // NS: temporary for performance tests
            // PriorityTask pTask = _dispatcher.schedule(
            // this, Priority.Low, getRttEstimate());
            PriorityTask pTask = dispatcher.schedule(this, Priority.Low, 10000);
            messages.put(this, pTask);
        }

        public void forceRetransmit() {
            // _logger.info("Early retransmit: " + _message);
            // if (readyMsgs.contains(this)) {
            // _logger.info("Already queued");
            // // Already schedule for retransmission
            // return;
            // }
            /*
             * Don't retransmit the message immediately, as sometimes messages
             * are delivered out of order. Wait for the estimate of a round
             * trip. If the message was scheduled to be transmitted earlier than
             * that, keep with that schedule.
             */
            PriorityTask handler = messages.get(this);
            long currentDelay = handler.getDelay();
            // Half of the estimated rtt
            long newDelay = (long) (ma.get() / 2);
            if (newDelay < currentDelay) {
                // Reduce the delay
                logger.info("Reducing retransmit delay from " + currentDelay + " to " + newDelay +
                             ", Msg: " + message);
                handler.cancel();
                handler = dispatcher.schedule(this, Priority.Low, newDelay);
                messages.put(this, handler);
            }
        }
    }

    // @Override
    // public void onIncomingQueueEmpty(){
    // // _logger.info("QueueEmpty");
    // while (!readyMsgs.isEmpty()) {
    // InnerRetransmittedMessage msg = readyMsgs.remove();
    // if (!msg.canceled) {
    // _logger.info("[QueueEmpty] Retransmitting: " + msg.getMessage() + " to "
    // + msg.getDestination());
    // }
    // msg.retransmit();
    // }
    //
    // }
    private final static Logger logger = Logger.getLogger(Retransmitter.class.getCanonicalName());
}
