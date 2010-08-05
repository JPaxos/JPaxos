// boo!
package lsr.paxos.network;

import java.io.IOException;
import java.util.BitSet;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

/**
 * This interface provides methods to communicate with other processes
 * (replicas). It allows to send the message to one or many replicas, and
 * provides listeners called every time new message is received or sent.
 * 
 */
public interface Network {
	static final boolean JAVA_SERIALIZATION = false;

	/**
	 * Sends the message to process with specified id.
	 * 
	 * @param message
	 *            the message to send
	 * @param destination
	 *            the id of replica to send message to
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	void sendMessage(Message message, int destination);

	/**
	 * Sends the message to process with id in the specified set.
	 * 
	 * @param message
	 *            the message to send
	 * @param destinations
	 *            the set of id's of replicas to send message to
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	void sendMessage(Message message, BitSet destinations);

	/**
	 * Sends the message to all processes.
	 * 
	 * @param message
	 *            the message to send
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	void sendToAll(Message message);

	// /**
	// * Registers new listener which will be called every time new message is
	// * sent or received.
	// *
	// * @param listener
	// * - the listener to register
	// */
	// @Deprecated
	// void addNetworkListener(NetworkListener listener);
	//
	// @Deprecated
	// void removeNetworkListener(NetworkListener listener);

	/**
	 * Registers new listener which will be called every time new message is
	 * sent or received.
	 * 
	 * @param mType
	 * @param handler
	 */
	void addMessageListener(MessageType mType, MessageHandler handler);

	/**
	 * Unregisters the listener from this network. It will not be called when
	 * new message is sent or received.
	 * 
	 * @param listener
	 *            - the listener to unregister
	 */
	void removeMessageListener(MessageType mType, MessageHandler handler);
}
