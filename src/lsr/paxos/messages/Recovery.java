package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Recovery extends Message {
    private static final long serialVersionUID = 1L;

    /** Processes epoch number */
    private final long epoch;

    public Recovery(DataInputStream input) throws IOException {
        super(input);
        epoch = input.readLong();
    }

    public Recovery(int view, long epoch) {
        super(view);
        this.epoch = epoch;
    }

    public MessageType getType() {
        return MessageType.Recovery;
    }

    protected void write(ByteBuffer bb) {
        bb.putLong(epoch);
    }

    public int byteSize() {
        return super.byteSize() + 8;
    }

    public long getEpoch() {
        return epoch;
    }

    public String toString() {
        return "Recovery(" + super.toString() + ",e=" + epoch + ")";
    }

}
