package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Base class for all messages. Every message requires to know the view number
 * of the sender.
 * <p>
 * To implement new message, it is required to override the
 * <code>byteSize()</code> method and implement <code>write()</code> method. See
 * subclasses implementation for details how they should be implemented.
 */
public abstract class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final int view;
    private long sentTime;

    /**
     * Creates message from specified view number and current time.
     * 
     * @param view - current view number
     */
    protected Message(int view) {
        this(view, System.currentTimeMillis());
    }

    /**
     * Creates new message from specified view number and sent time.
     * 
     * @param view - current view number
     * @param sentTime - time when the message was sent
     */
    protected Message(int view, long sentTime) {
        this.view = view;
        this.sentTime = sentTime;
    }

    /**
     * Creates new message from input stream with serialized message inside.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs
     */
    protected Message(DataInputStream input) throws IOException {
        view = input.readInt();
        sentTime = input.readLong();
    }

    /**
     * Sets the time when the message was sent.
     * 
     * @param sentTime - the time when the message was sent in milliseconds
     */
    public void setSentTime(long sentTime) {
        this.sentTime = sentTime;
    }

    /**
     * Returns the time when the message was sent.
     * 
     * @return the time when the message was sent in milliseconds.
     */
    public long getSentTime() {
        return sentTime;
    }

    /**
     * Sets the sent time to the current time.
     */
    public void setSentTime() {
        sentTime = System.currentTimeMillis();
    }

    /**
     * Returns the view number stored in this message.
     * 
     * @return the view number
     */
    public int getView() {
        return view;
    }

    /**
     * The size of the message after serialization in bytes.
     * 
     * @return the size of the message in bytes
     */
    public int byteSize() {
        return 1 + 4 + 8;
    }

    /**
     * Returns a message as byte array. The size of the array is equal to
     * <code>byteSize()</code>.
     * 
     * @return serialized message to byte array
     */
    public final byte[] toByteArray() {
        // Create with the byte array of the exact size,
        // to prevent internal resizes
        ByteBuffer bb = ByteBuffer.allocate(byteSize());
        bb.put((byte) getType().ordinal());
        bb.putInt(view);
        bb.putLong(sentTime);
        write(bb);

        assert bb.remaining() == 0 : "Wrong sizes. Limit=" + bb.limit() + ",capacity=" +
                                     bb.capacity() + ",position=" + bb.position();
        return bb.array();
    }

    /**
     * Returns the type of the message. This method is implemented by subclasses
     * and should return correct message type.
     * 
     * @return the type of the message
     */
    public abstract MessageType getType();

    public String toString() {
        return "v:" + getView();
    }

    /**
     * When serializing message to byte array, this function is called on the
     * message. Implementation of message-specific fields serialization must go
     * there.
     * 
     * @param bb - the byte buffer to serialize fields to
     */
    protected abstract void write(ByteBuffer bb);
}
