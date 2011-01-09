package lsr.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RequestIdTest {
    @Test
    public void shouldInitializeFields() {
        RequestId requestId = new RequestId(1, 2);
        assertEquals(1, (long) requestId.getClientId());
        assertEquals(2, requestId.getSeqNumber());
    }

    @Test
    public void shouldCompareTo() {
        RequestId first = new RequestId(1, 2);
        RequestId second = new RequestId(1, 3);

        assertTrue(first.compareTo(second) < 0);
        assertEquals(0, first.compareTo(first));
        assertTrue(second.compareTo(first) > 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCompareRequestsIdFromDifferentClients() {
        RequestId first = new RequestId(1, 2);
        RequestId second = new RequestId(2, 3);

        first.compareTo(second);
    }

    @Test
    public void shouldEqual() {
        RequestId requestId = new RequestId(1, 2);
        RequestId other = new RequestId(1, 2);

        assertFalse(requestId.equals(null));
        assertTrue(requestId.equals(requestId));
        assertTrue(requestId.equals(other));
        assertEquals(requestId.hashCode(), other.hashCode());
    }
}
