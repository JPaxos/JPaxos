package lsr.common;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * The reply to client request. It is send to client when replica execute this
 * command on state machine.
 * 
 * @see Request
 */
public class Reply implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RequestId requestId;
    private final byte[] value;

    /**
     * Creates new reply instance.
     * 
     * @param requestId - the id of request this reply is related to
     * @param value - result from state machine
     */
    public Reply(RequestId requestId, byte[] value) {
        assert requestId != null;
        assert value != null;
        this.requestId = requestId;
        this.value = value;
    }

    /**
     * Deserializes a <code>Reply</code> from the given byte array.
     * 
     * @param bytes - the bytes with serialized reply
     */
    public Reply(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Long clientId = buffer.getLong();
        int sequenceId = buffer.getInt();
        requestId = new RequestId(clientId, sequenceId);
        value = new byte[buffer.getInt()];
        buffer.get(value);
    }

    /**
     * Returns the id of request for which this reply is generated.
     * 
     * @return id of request
     */
    public RequestId getRequestId() {
        return requestId;
    }

    /**
     * Returns the total sum from state machine after executing request.
     * 
     * @return the sum from state machine
     */
    public byte[] getValue() {
        return value;
    }

    public byte[] toByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(byteSize());

        buffer.putLong(requestId.getClientId());
        buffer.putInt(requestId.getSeqNumber());
        buffer.putInt(value.length);
        buffer.put(value);

        return buffer.array();
    }

    /**
     * The size of the reply after serialization in bytes.
     * 
     * @return the size of the reply in bytes
     */
    public int byteSize() {
        int size = 8; // client ID
        size += 4; // sequential number
        size += 4; // value.length
        size += value.length; // value
        return size;
    }

    public String toString() {
        return requestId + " : " + (value == null ? "null" : ("Size: " + value.length));
    }
}
