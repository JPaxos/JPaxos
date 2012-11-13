package lsr.paxos;

import java.util.BitSet;

import lsr.paxos.messages.Message;

/**
 * Abstraction if a mechanism used to repeat transmission of passed messages as
 * long as either the {@link RetransmittedMessage} is stopped to all
 * destinations, or the Retransmitter is stopped.
 */
public interface Retransmitter {

    /**
     * Starts the process of handling retransmission. Messages are sent only if
     * the retransmitter has been started.
     */
    void init();

    /**
     * Starts retransmitting specified message to all processes except local
     * process. The message is sent immediately after calling this method.
     * 
     * @param message - the message to retransmit
     * @return the handler used to control retransmitting message
     */
    RetransmittedMessage startTransmitting(Message message);

    /**
     * Starts retransmitting specified message to processes specified in
     * destination parameter. The message is sent immediately after calling this
     * method.
     * 
     * @param message - the message to retransmit
     * @param destinations - bit set containing list of replicas to which
     *            message should be retransmitted
     * @return the handler used to control retransmitting message
     */
    RetransmittedMessage startTransmitting(Message message, BitSet destinations);

    /**
     * Stops retransmitting all messages.
     */
    void stopAll();

    /**
     * Disables retransmitter, opposite to #init().
     */
    void close();
}
