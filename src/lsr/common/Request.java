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
public class Request implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RequestId requestId;
    private final byte[] value;

    /**
     * Creates new <code>Request</code>.
     * 
     * @param requestId - id of this request
     * @param value - the value of request
     */
    public Request(RequestId requestId, byte[] value) {
        assert value != null : "Cannot create a request with a null value";
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

    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;

        Request request = (Request) obj;
        if (requestId == null)
            return request.requestId == null;

        if (requestId.equals(request.requestId)) {
            assert Arrays.equals(value, request.value) : "Critical: identical RequestID, different value";
            return true;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return requestId == null? 0 : requestId.hashCode();
    }

    public String toString() {
        return "id=" + requestId;
    }
}
