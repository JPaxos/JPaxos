package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.paxos.replica.ClientBatchID;

/**
 * Represents a message containing a batch of requests and the corresponding batch id.
 * Each batch is identified by a batch id, composed of <replicaID, localSeqNumber>.
 * 
 * Additionally, it piggybacks a vector <code>rcvdUB</code>, where <code>rcvdUB[q]</code> 
 * is the highest sequence number of a batch of requests received from <code>q</code> by 
 * the sender of this message.
 * 
 * @author Nuno Santos (LSR)
 */
public final class ForwardClientBatch extends Message {
    private static final long serialVersionUID = 1L;
    private static final int N = ProcessDescriptor.getInstance().numReplicas;
    
    public final ClientBatchID rid;
    public final ClientRequest[] requests;
    public final int[] rcvdUB;

    protected ForwardClientBatch(DataInputStream input) throws IOException {
        super(input);
        rid = new ClientBatchID(input);
        int size = input.readInt();
        requests = new ClientRequest[size];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = ClientRequest.create(input);
        }
        rcvdUB = new int[N];
        for (int i = 0; i < N; i++) {
            rcvdUB[i] = input.readInt();
        }
    }
    
    /** 
     * Warning: this constructor keeps a reference to the array <code>rcvdUB</code>.
     * Make sure that this array is not changed after calling this constructor.
     * @param id
     * @param requests
     * @param rcvdUB
     */
    public ForwardClientBatch(ClientBatchID id, ClientRequest[] requests, int[] rcvdUB) {
        super(-1);
        this.rid = id;
        this.requests = requests;
        this.rcvdUB = rcvdUB;
    }
    
    @Override
    public MessageType getType() {
        return MessageType.ForwardedClientRequest;
    }

    @Override
    protected void write(ByteBuffer bb) {
        rid.writeTo(bb);
        bb.putInt(requests.length);
        for (int i = 0; i < requests.length; i++) {
            requests[i].writeTo(bb);
        }
        for (int i = 0; i < rcvdUB.length; i++) {
            bb.putInt(rcvdUB[i]);
        }
    }
    
    public int byteSize() {
        int reqSize = 0;
        for (int i = 0; i < requests.length; i++) {
            reqSize+=requests[i].byteSize();
        }
        return super.byteSize() + rid.byteSize() + 4 + reqSize + 4*rcvdUB.length;
    }
    
    public String toString() {
        return ForwardClientBatch.class.getSimpleName() + "(rid:" + rid + ", " + Arrays.toString(requests) + ", " + Arrays.toString(rcvdUB) + ")";
    }
}
