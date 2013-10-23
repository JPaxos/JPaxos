package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides methods to communicate with other processes (replicas).
 * It allows to send the message to one or many replicas, and provides listeners
 * called every time new message is received or sent.
 * 
 */
public abstract class Network {

    protected static final int localId = processDescriptor.localId;

    public final static BitSet OTHERS = OTHERS_initializer();

    private static BitSet OTHERS_initializer() {
        BitSet bs = new BitSet(processDescriptor.numReplicas);
        bs.set(0, processDescriptor.numReplicas);
        bs.clear(processDescriptor.localId);
        return bs;
    }

    public Network() {
    }

    protected abstract void send(Message message, int destination);

    protected abstract void send(Message message, BitSet destinations);

    public abstract void start();

    /**
     * Sends the message to process with specified id.
     * 
     * @param message the message to send
     * @param destination the id of replica to send message to
     */
    final public void sendMessage(Message message, int destination) {
        assert destination != localId : "sending unicast to self";

        BitSet bs = new BitSet();
        bs.set(destination);

        send(message, destination);
        fireSentMessage(message, bs);
    }

    /**
     * Sends the message to processes with specified ids as a bitset
     * 
     * @param message the message to send
     * @param destinations bit set with marked replica id's to send message to
     */
    final public void sendMessage(Message message, BitSet destinations) {
        assert message != null : "Null message";
        assert !destinations.isEmpty() : "Sending a message to noone";
        assert !destinations.get(localId) : "sending to self is inefficient";

        if (logger.isTraceEnabled()) {
            logger.trace(
                    "Sending with {} message {} to {}",
                    this.getClass().getName().substring(
                            this.getClass().getName().lastIndexOf('.') + 1), message, destinations);
        }

        send(message, destinations);
        fireSentMessage(message, destinations);
    }

    /**
     * Sends the message to all processes but the sender
     * 
     * @param message the message to send
     */
    final public void sendToOthers(Message message) {
        sendMessage(message, OTHERS);
    }

    /**
     * Adds a new message listener for a certain type of message or all messages
     * ( see {@link MessageType}). The listener cannot be added twice for the
     * same message - this causes a {@link RuntimeException}.
     */
    final public static void addMessageListener(MessageType mType, MessageHandler handler) {
        boolean wasAdded = false;
        synchronized (msgListeners) {
            if (mType == MessageType.ANY) {
                for (Entry<MessageType, CopyOnWriteArrayList<MessageHandler>> entry : msgListeners.entrySet()) {
                    if (entry.getKey() == MessageType.ANY || entry.getKey() == MessageType.SENT)
                        continue;
                    wasAdded = entry.getValue().addIfAbsent(handler);
                    if (!wasAdded) {
                        throw new RuntimeException("Handler already registered");
                    }
                }
            } else {
                CopyOnWriteArrayList<MessageHandler> handlers = msgListeners.get(mType);
                wasAdded = handlers.addIfAbsent(handler);
            }
        }
        if (!wasAdded) {
            throw new RuntimeException("Handler already registered");
        }
    }

    /**
     * Removes a previously registered listener. Throws {@link RuntimeException}
     * if the listener is not on list.
     */
    final public static void removeMessageListener(MessageType mType, MessageHandler handler) {
        boolean wasPresent = false;
        synchronized (msgListeners) {
            if (mType == MessageType.ANY) {
                for (Entry<MessageType, CopyOnWriteArrayList<MessageHandler>> entry : msgListeners.entrySet()) {
                    if (entry.getKey() == MessageType.ANY || entry.getKey() == MessageType.SENT)
                        continue;
                    wasPresent |= entry.getValue().remove(handler);
                }
            } else {
                CopyOnWriteArrayList<MessageHandler> handlers = msgListeners.get(mType);
                wasPresent = handlers.remove(handler);
            }
        }
        if (!wasPresent) {
            throw new RuntimeException("Handler not registered");
        }
    }

    // // // // // // // // // // // // // // // // // //

    /**
     * For each message type, keeps a list of it's listeners.
     * 
     * The list is shared between networks
     */
    private static final Map<MessageType, CopyOnWriteArrayList<MessageHandler>> msgListeners;
    static {
        msgListeners = Collections.synchronizedMap(
            new EnumMap<MessageType, CopyOnWriteArrayList<MessageHandler>>(MessageType.class));
        for (MessageType ms : MessageType.values()) {
            msgListeners.put(ms, new CopyOnWriteArrayList<MessageHandler>());
        }
    }

    /**
     * Notifies all active network listeners that new message was received.
     */
    protected final void fireReceiveMessage(Message message, int sender) {
        assert message.getType() != MessageType.SENT && message.getType() != MessageType.ANY;
        if (logger.isTraceEnabled()) {
            StackTraceElement[] st = Thread.currentThread().getStackTrace();
            String className = st[st.length - 2].getClassName().substring(
                    st[st.length - 2].getClassName().lastIndexOf('.') + 1);
            logger.trace("Received from [p{}] by {} message {}", sender, className, message);
        }
        boolean handled = broadcastToListeners(message.getType(), message, sender);
        if (!handled) {
            logger.warn("Unhandled message: " + message);
        }
    }

    /**
     * Notifies all active network listeners that message was sent.
     */
    private final void fireSentMessage(Message msg, BitSet dest) {
        List<MessageHandler> handlers = msgListeners.get(MessageType.SENT);
        for (MessageHandler listener : handlers) {
            listener.onMessageSent(msg, dest);
        }
        if (logger.isTraceEnabled()) {
            StackTraceElement[] st = Thread.currentThread().getStackTrace();
            String stel = st[st.length - 2].toString();
            logger.trace("Sending message {} to {} from {}", msg, dest, stel);
        }
    }

    /**
     * Informs all listeners waiting for the message type about the message.
     * Parameter type is needed in order to support MessageType.ANY value.
     * Returns if there was at least one listener.
     */
    private final boolean broadcastToListeners(MessageType type, Message msg, int sender) {
        List<MessageHandler> handlers = msgListeners.get(type);
        boolean handled = false;
        for (MessageHandler listener : handlers) {
            listener.onMessageReceived(msg, sender);
            handled = true;
        }
        return handled;
    }

    private final static Logger logger = LoggerFactory.getLogger(Network.class);
}
