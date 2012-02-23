package lsr.paxos.replica;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

final public class ReplicaRequestID {
    public static final ReplicaRequestID NOP = new ReplicaRequestID(-1, -1);;
    
    public final int replicaID;
    public final int sn;
    
    public ReplicaRequestID(int replicaID, int sequenceNumber) {
        this.replicaID = replicaID;
        this.sn = sequenceNumber;
    }

    public ReplicaRequestID(DataInputStream input) throws IOException {
        this.replicaID = input.readInt();
        this.sn = input.readInt();
    }

    public ReplicaRequestID(ByteBuffer buffer) {
        this.replicaID = buffer.getInt();
        this.sn = buffer.getInt();
    }

    public int byteSize() {
        return 4+4;
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
        return this == NOP;
    }
    
}
