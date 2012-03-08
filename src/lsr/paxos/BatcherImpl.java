package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import lsr.common.ClientRequest;

public class BatcherImpl implements Batcher {
    public Deque<ClientRequest> unpack(byte[] source) {
        ByteBuffer bb = ByteBuffer.wrap(source);
        int count = bb.getInt();

        Deque<ClientRequest> requests = new ArrayDeque<ClientRequest>(count);

        for (int i = 0; i < count; ++i) {
            requests.add(ClientRequest.create(bb));
        }

        assert bb.remaining() == 0 : "Packing/unpacking error";

        return requests;
    }
}
