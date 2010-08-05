package lsr.paxos.network;

import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public abstract class AbstractNetwork implements Network {

	/* By using a synchronized collection and CopyOnWriteArrayList as elements,
	 * there is no need to acquire other locks to manipulate these lists 
	 */
	/** For each message type, keeps a list of listeners*/
	private final Map<MessageType, CopyOnWriteArrayList<MessageHandler>> _msgListeners =
		Collections.synchronizedMap(
				new EnumMap<MessageType, CopyOnWriteArrayList<MessageHandler>>(MessageType.class));

	public AbstractNetwork() {
		// Initialize the enum map with empty arrays. Simplifies the rest of the code
		for (MessageType ms : MessageType.values()) {
			_msgListeners.put(ms, new CopyOnWriteArrayList<MessageHandler>());
		}
	}

	/**
	 * Notifies all active network listeners that new message was received.
	 * 
	 * @param message
	 *            - message received from network
	 * @param sender
	 *            - id of replica from which message was received
	 */
	protected final void fireReceiveMessage(Message message, int sender) {
		assert message.getType() != MessageType.SENT && message.getType() != MessageType.ANY;
		boolean handled = 
			broadcastToListeners(message.getType(), message, sender);
//		// Do not notify twice if this method is called with ANY 
//		if (message.getType() != MessageType.ANY) {
		handled |= 
			broadcastToListeners(MessageType.ANY, message, sender);
		if (!handled) {
			_logger.fine("Unhandled message: " + message);
		}
//		}
	}

	private final boolean broadcastToListeners(MessageType type, Message msg, int sender) {
		List<MessageHandler> handlers = _msgListeners.get(type);
		boolean handled = false;
//		_logger.info(this + " Message: " + msg + " (as "+ type + ") from " + sender);
		for (MessageHandler listener : handlers) {
//			_logger.info(this + " Calling listener: " + listener);
			listener.onMessageReceived(msg, sender);
			handled = true;
		}
		return handled;
	}

	/**
	 * Notifies all active network listeners that message was sent.
	 * 
	 * @param Message sent
	 * @param dest id's of destinations to which message was sent
	 */
	protected final void fireSentMessage(Message msg, BitSet dest) {
		List<MessageHandler> handlers = _msgListeners.get(MessageType.SENT);
		for (MessageHandler listener : handlers) {
			listener.onMessageSent(msg, dest);
		}
	}	

	/**
	 *   
	 */
	public void addMessageListener(MessageType mType, MessageHandler handler)  {
//		System.out.println("Adding handler: " + handler + " - " + mType);
		CopyOnWriteArrayList<MessageHandler> handlers = _msgListeners.get(mType);
		boolean wasAdded = handlers.addIfAbsent(handler);
		if (!wasAdded) {
			throw new RuntimeException("Handler already registered");
		}
	}

	public void removeMessageListener(MessageType mType, MessageHandler handler) {
//		System.out.println("Removing handler: " + handler + " - " + mType);
		CopyOnWriteArrayList<MessageHandler> handlers = _msgListeners.get(mType);
		boolean wasPresent = handlers.remove(handler);
		if (!wasPresent) {
			throw new RuntimeException("Handler not registered");
		}
	}
	private final static Logger _logger = Logger.getLogger(AbstractNetwork.class.getCanonicalName());
}
