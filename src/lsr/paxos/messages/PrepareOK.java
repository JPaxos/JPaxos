package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.paxos.storage.ConsensusInstance;

/**
 * Represents the response to <code>Prepare</code> message. It contains view
 * number and list of consensus instances. If recovery with epoch vector is
 * used, then epoch vector is also sent.
 */
public class PrepareOK extends Message {
    private static final long serialVersionUID = 1L;
    private final ConsensusInstance[] prepared;
    private final long[] epoch;

    /**
     * Creates new <code>PrepareOK</code> message with epoch vector.
     * 
     * @param view - sender view number
     * @param prepared - list of prepared consensus instances
     * @param epoch - the epoch vector
     */
    public PrepareOK(int view, ConsensusInstance[] prepared, long[] epoch) {
        super(view);
        assert epoch != null;
        this.prepared = prepared;
        this.epoch = epoch;
    }

    /**
     * Creates new <cod>PrepareOK</code> message from serialized stream.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs
     */
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

    /**
     * Returns prepared list of consensus instances.
     * 
     * @return prepared list of consensus instances.
     */
    public ConsensusInstance[] getPrepared() {
        return prepared;
    }

    /**
     * Returns epoch vector. This value should never be equal to
     * <code>null</code>. If this message doesn't contain epoch vector, then it
     * will be represented as empty array.
     * 
     * @return epoch vector
     */
    public long[] getEpoch() {
        return epoch;
    }

    public MessageType getType() {
        return MessageType.PrepareOK;
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

    protected void write(ByteBuffer bb) {
        bb.putInt(prepared.length);
        for (ConsensusInstance ci : prepared) {
            ci.write(bb);
        }

        bb.putInt(epoch.length);
        for (int i = 0; i < epoch.length; ++i) {
            bb.putLong(epoch[i]);
        }
    }
}
