package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Deque;

import lsr.common.Request;
import lsr.common.RequestId;

import org.junit.Test;

public class BatcherImplTest {
    @Test
    public void shouldUnPackRequests() {
        Batcher batcher = new BatcherImpl();

        Request request1 = new Request(new RequestId(1, 2), new byte[] {1, 2, 3});
        Request request2 = new Request(new RequestId(3, 4), new byte[] {1, 2, 3, 4});
        ByteBuffer buffer = ByteBuffer.allocate(4 + request1.byteSize() + request2.byteSize());
        buffer.putInt(2);
        request1.writeTo(buffer);
        request2.writeTo(buffer);

        Deque<Request> requests = batcher.unpack(buffer.array());
        Request actual1 = requests.pollFirst();
        Request actual2 = requests.pollFirst();

        assertTrue(requests.isEmpty());
        assertEquals(request1, actual1);
        assertEquals(request2, actual2);
    }
}
