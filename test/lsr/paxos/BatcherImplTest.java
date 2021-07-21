package lsr.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

import lsr.common.ClientRequest;
import lsr.common.RequestId;

public class BatcherImplTest {
    @Test
    public void shouldUnPackRequests() {

        ClientRequest request1 = new ClientRequest(new RequestId(1, 2), new byte[] {1, 2, 3});
        ClientRequest request2 = new ClientRequest(new RequestId(3, 4), new byte[] {1, 2, 3, 4});
        ByteBuffer buffer = ByteBuffer.allocate(4 + request1.byteSize() + request2.byteSize());
        buffer.putInt(2);
        request1.writeTo(buffer);
        request2.writeTo(buffer);

        ClientRequest[] requests = UnBatcher.unpackCR(buffer.array());
        ClientRequest actual1 = requests[0];
        ClientRequest actual2 = requests[1];

        assertTrue(requests.length == 2);
        assertEquals(request1, actual1);
        assertEquals(request2, actual2);
    }
}
