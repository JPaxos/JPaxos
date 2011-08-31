package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.paxos.replica.ReplicaRequestID;

public final class ForwardClientRequest extends Message {
    private static final long serialVersionUID = 1L;
    private static final int N = ProcessDescriptor.getInstance().numReplicas;
    
    public final ClientRequest request;
    public final ReplicaRequestID id;
    public final int[] rcvdUB = new int[N];

    protected ForwardClientRequest(DataInputStream input) throws IOException {
        super(input);
        request = ClientRequest.create(input);
        id = new ReplicaRequestID(input);
        for (int i = 0; i < N; i++) {
            rcvdUB[i] = input.readInt();
        }
    }
    
    public ForwardClientRequest(ClientRequest request, ReplicaRequestID id, int[] rcvdUB) {
        super(-1);
        this.request = request;
        this.id = id;
        System.arraycopy(rcvdUB, 0, this.rcvdUB, 0, N);
    }
    
    @Override
    public MessageType getType() {
        return MessageType.ForwardedClientRequest;
    }

    @Override
    protected void write(ByteBuffer bb) {
        request.writeTo(bb);
        id.writeTo(bb);
        for (int i = 0; i < rcvdUB.length; i++) {
            bb.putInt(rcvdUB[i]);
        }
    }
    
    public int byteSize() {
        return super.byteSize() + request.byteSize() + id.byteSize() + 4*rcvdUB.length;
    }
    
    public String toString() {
        return ForwardClientRequest.class.getSimpleName() + "(" + super.toString() + ", rid:" + id + ", " + request + ", " + Arrays.toString(rcvdUB) + ")";
    }
}
