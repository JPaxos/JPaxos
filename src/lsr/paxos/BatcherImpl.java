package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import lsr.common.ReplicaRequest;

public class BatcherImpl implements Batcher {
    public Deque<ReplicaRequest> unpack(byte[] source) {
        ByteBuffer bb = ByteBuffer.wrap(source);
        int count = bb.getInt();

        Deque<ReplicaRequest> requests = new ArrayDeque<ReplicaRequest>(count);

        for (int i = 0; i < count; ++i) {
            requests.add(ReplicaRequest.create(bb));
        }

        assert bb.remaining() == 0 : "Packing/unpacking error";

        return requests;
    }
}
