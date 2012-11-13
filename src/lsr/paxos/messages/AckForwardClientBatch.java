package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.common.ProcessDescriptor;

/**
 * Contains a vector <code>upper</code>, where <code>upper[r]</code> is the
 * highest sequence number of the batches the local replica received from
 * replica r.
 * 
 * @author Nuno Santos (LSR)
 */
final public class AckForwardClientBatch extends Message {
    private static final long serialVersionUID = 1L;
    private static final int N = ProcessDescriptor.getInstance().numReplicas;

    public final int[] rcvdUB = new int[N];

    public AckForwardClientBatch(DataInputStream input) throws IOException {
        super(input);
        for (int i = 0; i < N; i++) {
            rcvdUB[i] = input.readInt();
        }
    }

    public AckForwardClientBatch(int[] rcvdUB) {
        super(-1);
        System.arraycopy(rcvdUB, 0, this.rcvdUB, 0, N);
    }

    @Override
    public MessageType getType() {
        return MessageType.AckForwardedRequest;
    }

    @Override
    protected void write(ByteBuffer bb) {
        for (int i = 0; i < rcvdUB.length; i++) {
            bb.putInt(rcvdUB[i]);
        }
    }

    public int byteSize() {
        return super.byteSize() + 4 * rcvdUB.length;
    }

    @Override
    public String toString() {
        return AckForwardClientBatch.class.getSimpleName() + "(" + Arrays.toString(rcvdUB) + ")";
    }
}
