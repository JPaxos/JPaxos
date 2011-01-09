package lsr.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ReplyTest {
    @Test
    public void shouldSetFields() {
        RequestId requestId = new RequestId(1, 1);
        Reply reply = new Reply(requestId, new byte[] {1, 2, 3});

        assertEquals(requestId, reply.getRequestId());
        assertArrayEquals(new byte[] {1, 2, 3}, reply.getValue());
    }

    @Test
    public void shouldSerializeAndDeserialize() {
        RequestId requestId = new RequestId(1, 1);
        Reply reply = new Reply(requestId, new byte[] {1, 2, 3});

        byte[] serialized = reply.toByteArray();
        assertEquals(serialized.length, reply.byteSize());

        Reply actual = new Reply(serialized);
        assertEquals(reply.getRequestId(), actual.getRequestId());
    }
}
