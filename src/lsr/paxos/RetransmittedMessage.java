package lsr.paxos;

/**
 * Intended to be a class for controlling retransmitted messages.
 * 
 * Single object controls one message.
 * 
 * Lets stopping message retransmission to one destination, or to all.
 * 
 * The beginning of message re-sending should begin before (or as) the object is
 * created.
 */

public interface RetransmittedMessage {

    /**
     * Starts retransmitting to specified destination. If the retransmitter has
     * been stopped (by calling <code>stop()</code> method or by stopping all
     * destinations), the retransmitting will not be started again.
     * 
     * @param destination - the id of processes to transmit message again
     */
    void start(int destination);

    /**
     * Stops retransmitting message to specified process. After this method is
     * called, retransmitted message will not be send to
     * <code>destination</code>.
     * 
     * @param destination - id of replica
     */
    void stop(int destination);

    /**
     * Stops retransmitting a message. No more message will be sent.
     */
    void stop();
}
