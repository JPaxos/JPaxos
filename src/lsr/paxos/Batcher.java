package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import lsr.common.ClientRequest;
import lsr.paxos.replica.ClientBatchID;

public class Batcher {
    // Prevent construction
    private Batcher() {
    };

    public static Deque<ClientBatchID> unpackCBID(byte[] source) {
        ByteBuffer bb = ByteBuffer.wrap(source);
        int count = bb.getInt();

        Deque<ClientBatchID> requests = new ArrayDeque<ClientBatchID>(count);

        for (int i = 0; i < count; ++i) {
            requests.add(new ClientBatchID(bb));
        }

        assert bb.remaining() == 0 : "Packing/unpacking error";

        return requests;
    }

    public static ClientRequest[] unpackCR(byte[] source) {
        ByteBuffer bb = ByteBuffer.wrap(source);
        int count = bb.getInt();

        ClientRequest[] requests = new ClientRequest[count];

        for (int i = 0; i < count; ++i) {
            requests[i] = ClientRequest.create(bb);
        }

        assert bb.remaining() == 0 : "Packing/unpacking error";

        return requests;
    }
}
