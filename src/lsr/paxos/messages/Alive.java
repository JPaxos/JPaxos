package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents <code>Alive</code> message used by <code>FailureDetector</code> to
 * determine status of the current leader.
 */
public class Alive extends Message {
    private static final long serialVersionUID = 1L;
    /**
     * LogSize is the size of log (the highest started instance ID) of the
     * leader.
     */
    private final int logNextId;

    /**
     * Creates new <code>Alive</code> message with specified view number and
     * highest instance ID + 1.
     */
    public Alive(int view, int logNextId) {
        super(view);
        this.logNextId = logNextId;
    }

    /**
     * Creates new <code>Alive</code> message from input stream with serialized
     * message inside.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs
     */
    public Alive(DataInputStream input) throws IOException {
        super(input);
        logNextId = input.readInt();
    }

    /**
     * Returns the log next id from sender of this message.
     */
    public int getLogNextId() {
        return logNextId;
    }

    public MessageType getType() {
        return MessageType.Alive;
    }

    public int byteSize() {
        return super.byteSize() + 4;
    }

    public String toString() {
        return "ALIVE (" + super.toString() + ", logsize: " + logNextId + ")";
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(logNextId);
    }
}
