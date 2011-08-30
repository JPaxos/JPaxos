package lsr.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import lsr.paxos.replica.ReplicaRequestID;

/**
 * Represents the request of user which will be inserted into state machine
 * after deciding it. After executing this request, <code>Reply</code> message
 * is generated.
 * 
 * @see Reply
 */
public final class ReplicaRequest implements Serializable {
    /*
     * The Request class should be final. The custome deserialization does not
     * respect class hierarchy, so any class derived from request would be
     * deserialized as the base Request class, which could cause bugs if we rely
     * on type information in the code.
     */
    private static final long serialVersionUID = 1L;

    /** Represents the NOP request */
    public static final ReplicaRequest NOP = new ReplicaRequest(ReplicaRequestID.NOP);

    private final ReplicaRequestID rid;

    /**
     * Creates new <code>Request</code>.
     * 
     * @param requestId - id of this request. Must not be null.
     * @param value - the value of request. Must not be null (but may be empty).
     */
    public ReplicaRequest(ReplicaRequestID requestId) {
        assert requestId != null : "Request ID cannot be null";
        this.rid = requestId;
    }

    /**
     * Reads a request from the given <code>ByteBuffer</code> and advances the
     * position on the buffer.
     * 
     * @param buffer - the byte buffer with serialized request
     * @return deserialized request from input byte buffer
     */
    public static ReplicaRequest create(ByteBuffer buffer) {
        ReplicaRequestID rid = new ReplicaRequestID(buffer);
        return new ReplicaRequest(rid);
    }
    
    /** For use of ForwardedRequest class */
    public static ReplicaRequest create(DataInputStream input) throws IOException {
        ReplicaRequestID rid = new ReplicaRequestID(input);
        return new ReplicaRequest(rid);
    }

    /**
     * Returns the id of this request.
     * 
     * @return id of request
     */
    public ReplicaRequestID getRequestId() {
        return rid;
    }

    /**
     * The size of the request after serialization in bytes.
     * 
     * @return the size of the request in bytes
     */
    public int byteSize() {
        return rid.byteSize();
    }

    /**
     * Writes a message to specified byte buffer. The number of elements
     * remaining in specified buffer should be greater or equal than
     * <code>byteSize()</code>.
     * 
     * @param bb - the byte buffer to write message to
     */
    public void writeTo(ByteBuffer bb) {
        rid.writeTo(bb);
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
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ReplicaRequest request = (ReplicaRequest) obj;

        if (rid.equals(request.rid)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return rid.hashCode();
    }

    public String toString() {
        return "id=" + rid;
    }

    public boolean isNop() {
        return rid.isNop();
    }
}
