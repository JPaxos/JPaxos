package lsr.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class ClientRequest2Test {
    @Test
    public void shouldSetFields() {
        RequestId requestId = new RequestId(1, 1);
        ClientRequest request = new ClientRequest(requestId, new byte[] {1, 2, 3});

        assertEquals(requestId, request.getRequestId());
        assertArrayEquals(new byte[] {1, 2, 3}, request.getValue());
    }

    @Test
    public void shouldSerializeAndDeserialize() {
        RequestId requestId = new RequestId(1, 1);
        ClientRequest request = new ClientRequest(requestId, new byte[] {1, 2, 3});

        ByteBuffer byteBuffer = ByteBuffer.allocate(request.byteSize());
        request.writeTo(byteBuffer);

        assertFalse(byteBuffer.hasRemaining());

        byteBuffer.rewind();
        assertEquals(request, ClientRequest.create(byteBuffer));
    }

    @Test
    public void shouldEqualToTheSameRequest() {
        RequestId requestId1 = new RequestId(1, 1);
        ClientRequest request1 = new ClientRequest(requestId1, new byte[] {1, 2, 3});

        RequestId requestId2 = new RequestId(1, 1);
        ClientRequest request2 = new ClientRequest(requestId2, new byte[] {1, 2, 3});

        assertTrue(request1.equals(request2));
    }

    @Test
    public void shouldEqualWithItself() {
        RequestId requestId = new RequestId(1, 1);
        ClientRequest request = new ClientRequest(requestId, new byte[] {1, 2, 3});

        assertTrue(request.equals(request));
    }

    @Test
    public void shouldNotEqualWithDifferentRequest() {
        RequestId requestId1 = new RequestId(1, 1);
        ClientRequest request1 = new ClientRequest(requestId1, new byte[] {1, 2, 3});

        RequestId requestId2 = new RequestId(2, 1);
        ClientRequest request2 = new ClientRequest(requestId2, new byte[] {1, 2, 3});

        assertFalse(request1.equals(request2));
    }

    @Test
    public void shouldNotEqualWithDifferentObject() {
        RequestId requestId = new RequestId(1, 1);
        ClientRequest request = new ClientRequest(requestId, new byte[] {1, 2, 3});

        assertFalse(request.equals(new Object()));
    }
}
