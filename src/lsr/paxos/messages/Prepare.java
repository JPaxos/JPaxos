package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class Prepare extends Message {
    private static final long serialVersionUID = 1L;
    private final int firstUncommitted;

    /**
     * Request to prepare consensus instances higher or equal to
     * <code>firstUncommitted</code>.
     * 
     * @param view the view being prepared
     * @param firstUncommitted the first consensus instance for which this
     *            process doesn't know the decision.
     */
    public Prepare(int view, int firstUncommitted) {
        super(view);
        this.firstUncommitted = firstUncommitted;
    }

    public Prepare(DataInputStream input) throws IOException {
        super(input);
        firstUncommitted = input.readInt();
    }

    public int getFirstUncommitted() {
        return firstUncommitted;
    }

    public MessageType getType() {
        return MessageType.Prepare;
    }

    // protected void write(DataOutputStream os) throws IOException {
    // os.writeInt(_firstUncommitted);
    // }
    protected void write(ByteBuffer bb) throws IOException {
        bb.putInt(firstUncommitted);
    }

    public int byteSize() {
        return super.byteSize() + 4;
    }

    public String toString() {
        return "Prepare(" + super.toString() + ")";
    }
}
