package lsr.paxos;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.Dispatcher.Priority;
import lsr.common.MovingAverage;
import lsr.common.PriorityTask;
import lsr.common.ProcessDescriptor;
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
    private final int numReplicas;
    private final Map<InnerRetransmittedMessage, PriorityTask> messages =
            new HashMap<InnerRetransmittedMessage, PriorityTask>();
    private final static MovingAverage ma = new MovingAverage(0.1,
            ProcessDescriptor.getInstance().retransmitTimeout);

    /**
     * Initializes new instance of retransmitter.
     * 
     * @param network - the network used to send messages to other replicas
     * @param numReplicas - the number of processes (replicas)
     * @param dispatcher - the dispatcher used to schedule retransmit tasks
     */
    public Retransmitter(Network network, int numReplicas, Dispatcher dispatcher) {
        assert network != null;
        assert dispatcher != null;
        this.dispatcher = dispatcher;
        this.network = network;
        this.numReplicas = numReplicas;
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
        BitSet bs = new BitSet(numReplicas);
        bs.set(0, numReplicas);
        bs.clear(ProcessDescriptor.getInstance().localId);
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
    }

    private class InnerRetransmittedMessage implements RetransmittedMessage, Runnable {
        private final Message message;
        private final BitSet destination;

        /** Last retransmission time */
        private long sendTs;

        public InnerRetransmittedMessage(Message message, BitSet destination) {
            this.message = message;
            // the destination is cloned to not changing the original one while
            // stopping some destinations
            this.destination = (BitSet) destination.clone();
            sendTs = System.currentTimeMillis();
        }

        public void stop(int destination) {
            this.destination.clear(destination);
            if (this.destination.isEmpty()) {
                stop();
            }

            // Update moving average with how long it took
            // until this message stops being retransmitted
            ma.add(System.currentTimeMillis() - sendTs);
        }

        public void stop() {
            PriorityTask pTask = messages.remove(this);
            // The task may already have been canceled. If stop(destination) is
            // called for all destinations, the task is canceled.
            if (pTask != null) {
                pTask.cancel();
            }
            ma.add(System.currentTimeMillis() - sendTs);
        }

        public void start(int destination) {
            this.destination.set(destination);
        }

        public void run() {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Retransmitting " + message + " to " + destination);
            }

            // System is idle, retransmit immediately
            retransmit();
        }

        public void retransmit() {
            sendTs = System.currentTimeMillis();
            network.sendMessage(message, destination);
            // Schedule the next attempt
            // Impose a lower bound on retransmission frequency to prevent
            // excessive retransmission
            int nextAttemptTime = Math.max((int) (ma.get() * 3), 5);
            PriorityTask pTask = dispatcher.schedule(this, Priority.Low, nextAttemptTime);
            messages.put(this, pTask);
        }
    }

    private final static Logger logger = Logger.getLogger(Retransmitter.class.getCanonicalName());
}
