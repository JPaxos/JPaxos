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
    private final int logSize;

    /**
     * Creates new <code>Alive</code> message with specified view number and log
     * size.
     * 
     * @param view - the view number
     * @param logSize - the size of the log
     */
    public Alive(int view, int logSize) {
        super(view);
        this.logSize = logSize;
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
        logSize = input.readInt();
    }

    /**
     * Returns the log size from sender of this message.
     * 
     * @return the size of the log
     */
    public int getLogSize() {
        return logSize;
    }

    public MessageType getType() {
        return MessageType.Alive;
    }

    public int byteSize() {
        return super.byteSize() + 4;
    }

    public String toString() {
        return "ALIVE (" + super.toString() + ", logsize: " + logSize + ")";
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(logSize);
    }
}
