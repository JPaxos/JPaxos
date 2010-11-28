package lsr.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

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
        this.requestId = requestId;
        this.value = value;
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

    public String toString() {
        return requestId + ": Value=" + value;
    }

    public Reply(byte[] b) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(b);
            DataInputStream dis = new DataInputStream(bais);
            Long cid = dis.readLong();
            Integer sid;
            sid = dis.readInt();
            requestId = new RequestId(cid, sid);
            value = new byte[dis.readInt()];
            bais.read(value);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

    }

    public byte[] toByteArray() {
        ByteArrayOutputStream baos;
        try {
            baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(requestId.getClientId());
            dos.writeInt(requestId.getSeqNumber());
            dos.writeInt(value.length);
            baos.write(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    public int byteSize() {
        int size = 8; // client ID
        size += 4; // sequential number
        size += 4; // value.length
        size += value.length; // value
        return size;
    }
}
