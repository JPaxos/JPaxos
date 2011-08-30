package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.common.ClientRequest;
import lsr.paxos.replica.ReplicaRequestID;

public final class ForwardClientRequest extends Message {
    private static final long serialVersionUID = 1L;
    public final ClientRequest request;
    public final ReplicaRequestID id;

    protected ForwardClientRequest(DataInputStream input) throws IOException {
        super(input);
        request = ClientRequest.create(input);
        id = new ReplicaRequestID(input);
    }
    
    public ForwardClientRequest(ClientRequest request, ReplicaRequestID id) {
        super(-1);
        this.request = request;
        this.id = id;
    }
    
    @Override
    public MessageType getType() {
        return MessageType.ForwardedRequest;
    }

    @Override
    protected void write(ByteBuffer bb) {
        request.writeTo(bb);
        id.writeTo(bb);
    }
    
    public int byteSize() {
        return super.byteSize() + request.byteSize() + id.byteSize();
    }
    
    public String toString() {
        return ForwardClientRequest.class.getSimpleName() + "(" + super.toString() + ", " + id + " " + request + ")";
    }
}
