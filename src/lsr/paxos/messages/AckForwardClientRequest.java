package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.replica.ReplicaRequestID;

public class AckForwardClientRequest extends Message {
    private static final long serialVersionUID = 1L;
    
    public final ReplicaRequestID id;
    
    public AckForwardClientRequest(DataInputStream input) throws IOException {
        super(input);
        id = new ReplicaRequestID(input);
    }
    
    public AckForwardClientRequest(ReplicaRequestID id) {
        super(-1);
        this.id = id;
    }
    
    @Override
    public MessageType getType() {
        return MessageType.AckForwardedRequest;
    }

    @Override
    protected void write(ByteBuffer bb) {
        id.writeTo(bb);
    }
    
    public int byteSize() {
        return super.byteSize() + id.byteSize();
    }
    
    @Override
    public String toString() {
        return AckForwardClientRequest.class.getSimpleName() + "(" + super.toString() + ", " + id + ")";
    }

}
