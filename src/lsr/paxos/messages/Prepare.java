package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents request to prepare consensus instances higher or equal to
 * <code>firstUncommitted</code>.
 * 
 * @param view - the view being prepared
 * @param firstUncommitted - the first consensus instance for which this process
 *            doesn't know the decision.
 */
public final class Prepare extends Message {
    private static final long serialVersionUID = 1L;
    private final int firstUncommitted;

    /**
     * Creates new <code>Prepare</code> message. This is request to preparing
     * new view number.
     * 
     * @param view - the view being prepared
     * @param firstUncommitted - the first consensus instance for which sender
     *            process doesn't know the decision
     */
    public Prepare(int view, int firstUncommitted) {
        super(view);
        this.firstUncommitted = firstUncommitted;
    }

    /**
     * Creates new <code>Prepare</code> message from serialized stream.
     * 
     * @param input - the input stream with serialized message inside
     * @throws IOException if I/O error occurs
     */
    public Prepare(DataInputStream input) throws IOException {
        super(input);
        firstUncommitted = input.readInt();
    }

    /**
     * Returns id of first consensus instance sender does not know the decision
     * for.
     * 
     * @return id of first uncommitted consensus instance
     */
    public int getFirstUncommitted() {
        return firstUncommitted;
    }

    public MessageType getType() {
        return MessageType.Prepare;
    }

    public int byteSize() {
        return super.byteSize() + 4;
    }

    public String toString() {
        return "Prepare(" + super.toString() + ")";
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(firstUncommitted);
    }
}
