package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Base class for all messages. Contains sent time stamp and view number.
 */
public abstract class Message implements Serializable {
	private static final long serialVersionUID = 1L;
	protected final int _view;
	private long _sentTime;
	
	protected Message(int view) {
		this(view, System.currentTimeMillis());
	}

	protected Message(int view, long sentTime) {
		this._view = view;
		this._sentTime = sentTime;
	}

	protected Message(DataInputStream input) throws IOException {
		_view = input.readInt();
		_logger.fine("m0");
		_sentTime = input.readLong();
	}

	public void setSentTime(long sentTime) {
		_sentTime = sentTime;
	}

	public long getSentTime() {
		return _sentTime;
	}

	public void setSentTime() {
		_sentTime = System.currentTimeMillis();
	}

	public int getView() {
		return _view;
	}

	/**
	 * When writing message to byte[], this function is called on the message.
	 * Implementation of message-specific filed serialization must go there.
	 * 
	 * @param os
	 * @throws IOException
	 */
//	protected abstract void write(DataOutputStream os) throws IOException;
	protected abstract void write(ByteBuffer bb) throws IOException;
	
	public int byteSize() {
		return 1 + 4 + 8;
	}
	
	/**
	 * Returns a message as byte[]
	 * 
	 * @return
	 */
	public final byte[] toByteArray() {
		// Create with the byte array of the exact size, 
		// to prevent internal resizes
		ByteBuffer bb = ByteBuffer.allocate(byteSize());
//		ByteArrayOutputStream bas = new ByteArrayOutputStream(byteSize());
//		DataOutputStream os = new DataOutputStream(bas);
		try {
//			os.writeByte(getType().ordinal());
//			os.writeInt(_view);
//			os.writeLong(_sentTime);
//			write(os);
			bb.put((byte) getType().ordinal());
			bb.putInt(_view);
			bb.putLong(_sentTime);
			write(bb);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

//		return bas.toByteArray();
		assert bb.remaining() == 0 : "Wrong sizes. Limit="+bb.limit()+",capacity="+bb.capacity()+",position="+bb.position();
		return bb.array();
	}
	
	/**
	 * Must return valid type of the message
	 * 
	 * @return
	 */
	public abstract MessageType getType();
	
	public String toString() {
		return "v:" + getView();
	}
	
	private final static Logger _logger = Logger.getLogger(
			Message.class.getCanonicalName());

}

