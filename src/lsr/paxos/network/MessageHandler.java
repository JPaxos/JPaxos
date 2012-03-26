package lsr.paxos.network;

import java.util.BitSet;

import lsr.paxos.messages.Message;

/**
 * Implementations of this interface should always return quickly and in no
 * condition should block. Suggestion: enqueue the message in a local queue, so
 * it can be processed asynchronously.
 */
public interface MessageHandler {
    /**
     * This method should execute quickly and in no condition should block.
     * Suggestion: enqueue the message in a local queue, so it can be processed
     * asynchronously.
     * 
     * @param msg The message received.
     * @param sender The sender of the message
     */
    public void onMessageReceived(Message msg, int sender);

    /**
     * Callback method which is called every time new message was sent by the
     * network. We do not have the guarantee that the message reaches the
     * destinations.
     * 
     * @param message - message that was sent.
     * @param destinations - processes to which message was sent
     */

    public void onMessageSent(Message message, BitSet destinations);
}
