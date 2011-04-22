package lsr.common;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents the request of user which will be inserted into state machine
 * after deciding it. After executing this request, <code>Reply</code> message
 * is generated.
 * 
 * @see Reply
 */
public final class Request implements Serializable {
    /*
     * The Request class should be final. The custome deserialization does not
     * respect class hierarchy, so any class derived from request would be
     * deserialized as the base Request class, which could cause bugs if we rely
     * on type information in the code.
     */
    private static final long serialVersionUID = 1L;

    /** Represents the NOP request */
    public static final Request NOP = new Request(RequestId.NOP, new byte[0]);

    private final RequestId requestId;
    private final byte[] value;

    /**
     * Creates new <code>Request</code>.
     * 
     * @param requestId - id of this request. Must not be null.
     * @param value - the value of request. Must not be null (but may be empty).
     */
    public Request(RequestId requestId, byte[] value) {
        assert requestId != null : "Request ID cannot be null";
        assert value != null : "Value cannot be null";
        this.requestId = requestId;
        this.value = value;
    }

    /**
     * Reads a request from the given <code>ByteBuffer</code> and advances the
     * position on the buffer.
     * 
     * @param buffer - the byte buffer with serialized request
     * @return deserialized request from input byte buffer
     */
    public static Request create(ByteBuffer buffer) {
        Long clientId = buffer.getLong();
        int sequenceId = buffer.getInt();
        RequestId requestId = new RequestId(clientId, sequenceId);

        byte[] value = new byte[buffer.getInt()];
        buffer.get(value);
        return new Request(requestId, value);
    }

    /**
     * Returns the id of this request.
     * 
     * @return id of request
     */
    public RequestId getRequestId() {
        return requestId;
    }

    /**
     * Returns the value held by this request.
     * 
     * @return the value of this request
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * The size of the request after serialization in bytes.
     * 
     * @return the size of the request in bytes
     */
    public int byteSize() {
        return 8 + 4 + 4 + value.length;
    }

    /**
     * Writes a message to specified byte buffer. The number of elements
     * remaining in specified buffer should be greater or equal than
     * <code>byteSize()</code>.
     * 
     * @param bb - the byte buffer to write message to
     */
    public void writeTo(ByteBuffer bb) {
        bb.putLong(requestId.getClientId());
        bb.putInt(requestId.getSeqNumber());
        bb.putInt(value.length);
        bb.put(value);
    }

    /**
     * Creates a byte array with the binary representation of the request.
     * 
     * @return
     */
    public byte[] toByteArray() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteSize());
        writeTo(byteBuffer);
        return byteBuffer.array();
    }

    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;

        Request request = (Request) obj;

        if (requestId.equals(request.requestId)) {
            assert Arrays.equals(value, request.value) : "Critical: identical RequestID, different value";
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return requestId.hashCode();
    }

    public String toString() {
        return "id=" + requestId;
    }

    public boolean isNop() {
        return requestId.isNop();
    }
}
