package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.paxos.storage.ConsensusInstance;

public class PrepareOK extends Message {
    private static final long serialVersionUID = 1L;

    private final ConsensusInstance[] prepared;

    private final long[] epoch;

    public PrepareOK(int view, ConsensusInstance[] prepared) {
        this(view, prepared, new long[0]);
    }

    public PrepareOK(int view, ConsensusInstance[] prepared, long[] epoch) {
        super(view);
        this.prepared = prepared;
        this.epoch = epoch;
    }

    public PrepareOK(DataInputStream input) throws IOException {
        super(input);
        prepared = new ConsensusInstance[input.readInt()];
        for (int i = 0; i < prepared.length; ++i) {
            prepared[i] = new ConsensusInstance(input);
        }

        int epochSize = input.readInt();
        epoch = new long[epochSize];
        for (int i = 0; i < epoch.length; ++i) {
            epoch[i] = input.readLong();
        }
    }

    public ConsensusInstance[] getPrepared() {
        return prepared;
    }

    public long[] getEpoch() {
        return epoch;
    }

    public MessageType getType() {
        return MessageType.PrepareOK;
    }

    protected void write(ByteBuffer bb) throws IOException {
        bb.putInt(prepared.length);
        for (ConsensusInstance ci : prepared) {
            ci.write(bb);
        }

        bb.putInt(epoch.length);
        for (int i = 0; i < epoch.length; ++i) {
            bb.putLong(epoch[i]);
        }
    }

    public int byteSize() {
        int size = super.byteSize() + 4;
        for (ConsensusInstance ci : prepared) {
            size += ci.byteSize();
        }

        size += epoch.length * 8 + 4;

        return size;
    }

    public String toString() {
        return "PrepareOK(" + super.toString() + ", values: " + Arrays.toString(getPrepared()) +
               ")";
    }

}
