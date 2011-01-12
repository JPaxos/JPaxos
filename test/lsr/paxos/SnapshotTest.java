package lsr.paxos;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lsr.common.Reply;
import lsr.common.RequestId;

import org.junit.Before;
import org.junit.Test;

public class SnapshotTest {
    private Snapshot snapshot;

    @Before
    public void setUp() {
        snapshot = new Snapshot();
    }

    @Test
    public void shouldSetAndGetNextInstanceId() {
        assertEquals(0, snapshot.getNextInstanceId());

        snapshot.setNextInstanceId(5);

        assertEquals(5, snapshot.getNextInstanceId());
    }

    @Test
    public void shouldSetAndGetValue() {
        assertEquals(null, snapshot.getValue());

        byte[] value = new byte[] {1, 2, 4, 3};
        snapshot.setValue(value);

        assertEquals(value, snapshot.getValue());
    }

    @Test
    public void shouldSetAndGetLastReplyForClient() {
        assertEquals(null, snapshot.getLastReplyForClient());

        Map<Long, Reply> lastReplyForClient = new HashMap<Long, Reply>();
        snapshot.setLastReplyForClient(lastReplyForClient);

        assertEquals(lastReplyForClient, snapshot.getLastReplyForClient());
    }

    @Test
    public void shouldSetAndGetNextRequestSeqNo() {
        assertEquals(0, snapshot.getNextRequestSeqNo());

        snapshot.setNextRequestSeqNo(5);

        assertEquals(5, snapshot.getNextRequestSeqNo());
    }

    @Test
    public void shoudlSetAndGetStartingRequestSeqNo() {
        assertEquals(0, snapshot.getStartingRequestSeqNo());

        snapshot.setStartingRequestSeqNo(5);

        assertEquals(5, snapshot.getStartingRequestSeqNo());
    }

    @Test
    public void shouldSetAndGetPartialResponseCache() {
        assertEquals(null, snapshot.getPartialResponseCache());

        List<Reply> partialResponseCache = new ArrayList<Reply>();
        snapshot.setPartialResponseCache(partialResponseCache);

        assertEquals(partialResponseCache, snapshot.getPartialResponseCache());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(1);
        snapshot.setValue(new byte[] {1, 2, 3});
        snapshot.setLastReplyForClient(generateLastReplyForClient());
        snapshot.setNextRequestSeqNo(2);
        snapshot.setStartingRequestSeqNo(3);
        snapshot.setPartialResponseCache(generatePartialResponseCache());

        byte[] serializedUsingStream = serializeUsingStream(snapshot);
        assertEquals(serializedUsingStream.length, snapshot.byteSize());

        ByteBuffer buffer = ByteBuffer.allocate(snapshot.byteSize());
        snapshot.writeTo(buffer);
        assertFalse(buffer.hasRemaining());

        assertArrayEquals(serializedUsingStream, buffer.array());

        ByteArrayInputStream bais = new ByteArrayInputStream(serializedUsingStream);
        Snapshot deserialized = new Snapshot(new DataInputStream(bais));

        assertEquals(snapshot.getNextInstanceId(), deserialized.getNextInstanceId());
        assertArrayEquals(snapshot.getValue(), deserialized.getValue());
        assertEquals(snapshot.getNextRequestSeqNo(), deserialized.getNextRequestSeqNo());
        assertEquals(snapshot.getStartingRequestSeqNo(), deserialized.getStartingRequestSeqNo());

        assertEquals(snapshot.getLastReplyForClient().size(),
                deserialized.getLastReplyForClient().size());
        assertEquals(snapshot.getPartialResponseCache().size(),
                deserialized.getPartialResponseCache().size());
    }

    @Test
    public void shouldCompareToAnotherSnapshot() {
        Snapshot smaller = new Snapshot();
        Snapshot bigger = new Snapshot();

        smaller.setNextInstanceId(5);
        bigger.setNextInstanceId(10);

        assertEquals(-1, smaller.compareTo(bigger));
        assertEquals(1, bigger.compareTo(smaller));

        smaller.setNextInstanceId(5);
        smaller.setNextRequestSeqNo(3);
        bigger.setNextInstanceId(5);
        bigger.setNextRequestSeqNo(4);

        assertEquals(-1, smaller.compareTo(bigger));
        assertEquals(0, smaller.compareTo(smaller));
        assertEquals(0, bigger.compareTo(bigger));
        assertEquals(1, bigger.compareTo(smaller));
    }

    private HashMap<Long, Reply> generateLastReplyForClient() {
        HashMap<Long, Reply> lastReplyForClient = new HashMap<Long, Reply>();
        lastReplyForClient.put((long) 5, new Reply(new RequestId(1, 1), new byte[] {4, 3, 2, 1}));
        lastReplyForClient.put((long) 6, new Reply(new RequestId(2, 2), new byte[] {4, 3, 2, 1}));
        return lastReplyForClient;
    }

    private List<Reply> generatePartialResponseCache() {
        List<Reply> partialResponseCache = new ArrayList<Reply>();
        partialResponseCache.add(new Reply(new RequestId(3, 3), new byte[] {5, 4, 3, 2, 1}));
        partialResponseCache.add(new Reply(new RequestId(4, 4), new byte[] {1, 2, 3, 4, 5}));

        return partialResponseCache;
    }

    private byte[] serializeUsingStream(Snapshot snapshot) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream snapshotStream = new DataOutputStream(baos);
        snapshot.writeTo(snapshotStream);

        return baos.toByteArray();
    }
}
