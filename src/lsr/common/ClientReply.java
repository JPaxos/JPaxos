package lsr.common;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Represents the reply message which replica send to client after handling
 * {@link ClientCommand} request.
 */
public class ClientReply implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Result result;
    private final byte[] value;

    /** The result type of this reply message */
    public enum Result {
        OK, NACK, REDIRECT
    };

    /**
     * Create new client reply.
     * 
     * @param result - type of reply
     * @param value - value for this reply
     */
    public ClientReply(Result result, byte[] value) {
        this.result = result;
        this.value = value;
    }

    /**
     * Returns the result of this reply.
     * 
     * @return result of reply
     */
    public Result getResult() {
        return result;
    }

    /**
     * Returns the value of this reply.
     * 
     * @return value of reply
     */
    public byte[] getValue() {
        return value;
    }

    public String toString() {
        return result + " : " + (value == null ? "null" : ("Size: " + value.length));
    }

    public ClientReply(DataInputStream input) throws IOException {
        result = Result.values()[input.readInt()];
        value = new byte[input.readInt()];
        input.readFully(value);

    }

    public byte[] toByteArray() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            write(dos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    public void write(DataOutputStream output) throws IOException {
        output.writeInt(result.ordinal());
        output.writeInt(value.length);
        output.write(value);
    }
}
