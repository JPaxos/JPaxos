package lsr.paxos.network;

import java.util.BitSet;

import lsr.paxos.messages.Message;

/**
 * Implementations of this interface should always return quickly and in no
 * condition should block. Suggestion: enqueue the message in a local queue, so
 * it can be processed asynchronously.
 */
public interface NetworkListener {
    /**
     * Callback method which is called every time new message was sent by the
     * network. We do not have the guarantee that the message reach the
     * destinations.
     * 
     * @param message - message that was sent.
     * @param destinations - processes to which message was sent
     */
    public void messageSent(BitSet destinations);

    /**
     * Callback method which is called every time new message was received by
     * the network.
     * 
     * @param message - received message by network
     * @param sender - the id of replica which send this message
     */
    public void messageReceived(Message message, int sender);
}
