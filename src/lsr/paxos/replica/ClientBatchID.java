package lsr.paxos.replica;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

final public class ClientBatchID {
    public static final ClientBatchID NOP = new ClientBatchID();

    public final int replicaID;
    public final int sn;

    /*
     * Used only to build the special NOP field. Bypasses error checking on the
     * public constructor
     */
    private ClientBatchID() {
        this.replicaID = -1;
        this.sn = -1;
    }

    public ClientBatchID(int replicaID, int sequenceNumber) {
        if (replicaID < 0 || sequenceNumber < 0)
            throw new IllegalArgumentException("Arguments must be non-negative. " +
                                               "Received: <replicaID:" + replicaID +
                                               ", sequenceNumber:" + sequenceNumber);
        this.replicaID = replicaID;
        this.sn = sequenceNumber;
    }

    public ClientBatchID(DataInputStream input) throws IOException {
        this.replicaID = input.readInt();
        this.sn = input.readInt();
    }

    public ClientBatchID(ByteBuffer buffer) {
        this.replicaID = buffer.getInt();
        this.sn = buffer.getInt();
    }

    public int byteSize() {
        return 4 + 4;
    }

    public void writeTo(ByteBuffer bb) {
        bb.putInt(replicaID);
        bb.putInt(sn);
    }

    @Override
    public String toString() {
        return replicaID + ":" + sn;
    }

    public boolean isNop() {
        /*
         * Reference equality is not enough, because the constructor that takes
         * a DataInputStream as an input violates the uniqueness of the NOP
         * object, as it will create a new instance of the class to represent
         * the NOP request.
         */
        return this.replicaID == NOP.replicaID && this.sn == NOP.sn;
    }

    @Override
    public boolean equals(Object other) {
        /* Adapted from Effective Java, Item 8 */
        if (!(other instanceof ClientBatchID))
            return false;
        ClientBatchID rid = (ClientBatchID) other;
        return rid.replicaID == replicaID && rid.sn == sn;
    }

    @Override
    public int hashCode() {
        /* Adapted from Effective Java, Item 9 */
        int result = 17;
        result = 31 * result + replicaID;
        result = 31 * result + sn;
        return result;
    }
}
