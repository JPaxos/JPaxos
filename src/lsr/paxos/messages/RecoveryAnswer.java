package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RecoveryAnswer extends Message {
    private static final long serialVersionUID = 1L;

    private final long[] epoch;
    private final long nextId;

    public RecoveryAnswer(int view, long[] epoch, long nextId) {
        super(view);
        this.epoch = epoch;
        this.nextId = nextId;
    }

    public RecoveryAnswer(int view, long nextId) {
        super(view);
        this.epoch = new long[0];
        this.nextId = nextId;
    }

    public RecoveryAnswer(DataInputStream input) throws IOException {
        super(input);

        int epochSize = input.readInt();
        epoch = new long[epochSize];
        for (int i = 0; i < epoch.length; ++i) {
            epoch[i] = input.readLong();
        }

        nextId = input.readLong();
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(epoch.length);
        for (int i = 0; i < epoch.length; ++i) {
            bb.putLong(epoch[i]);
        }

        bb.putLong(nextId);
    }

    public int byteSize() {
        int size = super.byteSize();
        size += 4 + epoch.length * 8; // epoch
        size += 8; // nextId
        return size;
    }

    public long[] getEpoch() {
        return epoch;
    }

    public MessageType getType() {
        return MessageType.RecoveryAnswer;
    }

    public long getNextId() {
        return nextId;
    }

    public String toString() {
        return "RecoveryAnswer(" + super.toString() + ",e=" + Arrays.toString(epoch) + ",next=" +
               nextId + ")";
    }

}
