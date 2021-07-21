package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.common.ClientRequest;

/**
 * Represents a message containing a batch of requests and the corresponding
 * batch id. Each batch is identified by a batch id, composed of <replicaID,
 * localSeqNumber>.
 */
public final class ForwardClientRequests extends Message {
    private static final long serialVersionUID = 1L;

    private final ClientRequest[] requests;

    protected ForwardClientRequests(DataInputStream input) throws IOException {
        super(input);
        int size = input.readInt();
        requests = new ClientRequest[size];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = ClientRequest.create(input);
        }
    }

    public ForwardClientRequests(ByteBuffer bb) {
        super(bb);
        int size = bb.getInt();
        requests = new ClientRequest[size];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = ClientRequest.create(bb);
        }
    }

    /**
     * Warning: this constructor keeps a reference to the array
     * <code>rcvdUB</code>. Make sure that this array is not changed after
     * calling this constructor.
     * 
     * @param id
     * @param requests
     * @param rcvdUB
     */
    public ForwardClientRequests(ClientRequest[] requests) {
        super(-1);
        this.requests = requests;
    }

    public MessageType getType() {
        return MessageType.ForwardedClientRequests;
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(requests.length);
        for (int i = 0; i < requests.length; i++) {
            requests[i].writeTo(bb);
        }
    }

    public int byteSize() {
        int reqSize = 0;
        for (int i = 0; i < requests.length; i++) {
            reqSize += requests[i].byteSize();
        }
        return super.byteSize() + 4 + reqSize;
    }

    public String toString() {
        return super.toString() + " (" + Arrays.toString(requests) + ")";
    }

    public ClientRequest[] getRequests() {
        return requests;
    }
}
