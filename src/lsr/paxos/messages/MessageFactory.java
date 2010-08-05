package lsr.paxos.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Logger;

import lsr.common.Config;

/**
 * Dull class created only to separate message deserialization
 */
public class MessageFactory {

	public static Message readByteArray(byte[] message) {
		DataInputStream input = new DataInputStream(new ByteArrayInputStream(
				message));

		return create(input);
	}

	public static Message create(DataInputStream input) {
		if (Config.javaSerialization) {
			try {
				return (Message) (new ObjectInputStream(input).readObject());
			} catch (IOException e) {
				throw new IllegalArgumentException(
						"Exception deserializing message occured!", e);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(
						"Exception deserializing message occured!", e);
			}
		}
		return createMine(input);
	}

	/**
	 * Reads byte [] and creates ob basis of it a Message. Byte[] must have been
	 * written by Message::toByteArray() (or by some supernatural force knowing
	 * byte convention)
	 * 
	 * @param message
	 *            - contains the message to read
	 * @return correct object from one of message subclasses
	 * 
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws IllegalArgumentException
	 * @throws IllegalArgumentException
	 *             If a correct message could not be read from input
	 */
	private static Message createMine(DataInputStream input)
			throws IllegalArgumentException {
		MessageType type;
		Message m;

		try {
			type = MessageType.values()[input.readUnsignedByte()];
			// _logger.fine("mf0");
			m = type.newInstance(input);

		} catch (EOFException e) {
			_logger.severe("EOFException - probably a stream peer is down");
			throw new IllegalArgumentException(e);
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Exception deserializing message occured!", e);
		}

		return m;
	}

	public static byte[] serialize(Message message) {
		byte[] data;
		if (Config.javaSerialization)
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				new ObjectOutputStream(baos).writeObject(message);
				data = baos.toByteArray();
			} catch (IOException e) {
				throw new IllegalArgumentException(
						"Exception deserializing message occured!", e);
			}
		else {
			data = message.toByteArray();
		}
		return data;
	}

	private final static Logger _logger = Logger.getLogger(MessageFactory.class
			.getCanonicalName());
}
