package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.common.ProcessDescriptor;
import lsr.paxos.replica.Replica.CrashModel;
import lsr.paxos.storage.ConsensusInstance;

public class PrepareOK extends Message {
    private static final long serialVersionUID = 1L;

    private final ConsensusInstance[] prepared;

    private final long[] epoch;

    public PrepareOK(int view, ConsensusInstance[] prepared) {
        this(view, prepared, null);
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

        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();
        if (descriptor.crashModel == CrashModel.EpochSS) {
            epoch = new long[descriptor.numReplicas];
            for (int i = 0; i < descriptor.numReplicas; ++i) {
                epoch[i] = input.readLong();
            }
        } else
            epoch = null;
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
        
        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();
        if (descriptor.crashModel == CrashModel.EpochSS) {
            for (int i = 0; i < descriptor.numReplicas; ++i) {
                bb.putLong(epoch[i]);
            }
        }
    }

    public int byteSize() {
        int size = super.byteSize() + 4;
        for (ConsensusInstance ci : prepared) {
            size += ci.byteSize();
        }

        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();
        if (descriptor.crashModel == CrashModel.EpochSS) {
            size += descriptor.numReplicas;
        }

        return size;
    }

    public String toString() {
        return "PrepareOK(" + super.toString() + ", values: " + Arrays.toString(getPrepared()) +
               ")";
    }

}
