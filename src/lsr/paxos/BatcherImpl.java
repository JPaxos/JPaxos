package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import lsr.common.Request;

public class BatcherImpl implements Batcher {
    public Deque<Request> unpack(byte[] source) {
        ByteBuffer bb = ByteBuffer.wrap(source);
        int count = bb.getInt();

        Deque<Request> requests = new ArrayDeque<Request>(count);

        for (int i = 0; i < count; ++i) {
            requests.add(Request.create(bb));
        }

        assert bb.remaining() == 0 : "Packing/unpacking error";

        return requests;
    }
}
