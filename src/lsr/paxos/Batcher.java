package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import lsr.common.ClientBatch;

public class Batcher {
    // Prevent construction
    private Batcher() {};
    
    public static Deque<ClientBatch> unpack(byte[] source) {
        ByteBuffer bb = ByteBuffer.wrap(source);
        int count = bb.getInt();

        Deque<ClientBatch> requests = new ArrayDeque<ClientBatch>(count);

        for (int i = 0; i < count; ++i) {
            requests.add(ClientBatch.create(bb));
        }

        assert bb.remaining() == 0 : "Packing/unpacking error";

        return requests;
    }
}
