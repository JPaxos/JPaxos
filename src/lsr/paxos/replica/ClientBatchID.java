package lsr.paxos.replica;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

final public class ClientBatchID {
    public static final ClientBatchID NOP = new ClientBatchID();

    protected final int uniqueRunId;
    protected final int sn;

    /*
     * Used only to build the special NOP field. Bypasses error checking on the
     * public constructor
     */
    private ClientBatchID() {
        this.uniqueRunId = -1;
        this.sn = -1;
    }

    public ClientBatchID(int uniqueRunId, int sequenceNumber) {
        if (uniqueRunId < 0 || sequenceNumber < 0)
            throw new IllegalArgumentException("Arguments must be non-negative. " +
                                               "Received: <replicaID:" + uniqueRunId +
                                               ", sequenceNumber:" + sequenceNumber);
        this.uniqueRunId = uniqueRunId;
        this.sn = sequenceNumber;
    }

    public ClientBatchID(DataInputStream input) throws IOException {
        this.uniqueRunId = input.readInt();
        this.sn = input.readInt();
    }

    public ClientBatchID(ByteBuffer buffer) {
        this.uniqueRunId = buffer.getInt();
        this.sn = buffer.getInt();
    }

    public int byteSize() {
        return 4 + 4;
    }

    public static int byteSizeS() {
        return NOP.byteSize();
    }

    public void writeTo(DataOutputStream dos) throws IOException {
        dos.writeInt(uniqueRunId);
        dos.writeInt(sn);
    }

    public void writeTo(ByteBuffer bb) {
        bb.putInt(uniqueRunId);
        bb.putInt(sn);
    }

    @Override
    public String toString() {
        return uniqueRunId + ":" + sn;
    }

    public boolean isNop() {
        /*
         * Reference equality is not enough, because the constructor that takes
         * a DataInputStream as an input violates the uniqueness of the NOP
         * object, as it will create a new instance of the class to represent
         * the NOP request.
         */
        return this.uniqueRunId == NOP.uniqueRunId && this.sn == NOP.sn;
    }

    @Override
    public boolean equals(Object other) {
        /* Adapted from Effective Java, Item 8 */
        if (!(other instanceof ClientBatchID))
            return false;
        ClientBatchID rid = (ClientBatchID) other;
        return rid.uniqueRunId == uniqueRunId && rid.sn == sn;
    }

    @Override
    public int hashCode() {
        /* Adapted from Effective Java, Item 9 */
        int result = 17;
        result = 31 * result + uniqueRunId;
        result = 31 * result + sn;
        return result;
    }
}
