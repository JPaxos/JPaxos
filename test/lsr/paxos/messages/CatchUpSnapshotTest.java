package lsr.paxos.messages;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import lsr.common.Reply;
import lsr.paxos.Snapshot;

public class CatchUpSnapshotTest extends AbstractMessageTestCase<CatchUpSnapshot> {
    private int view = 12;
    private long requestTime = 32485729;
    private byte[] value = new byte[] {1, 7, 4, 5};
    private int instanceId = 52;
    private Snapshot snapshot;
    private CatchUpSnapshot catchUpSnapshot;

    @Before
    public void setUp() {
        snapshot = new Snapshot();
        snapshot.setNextInstanceId(instanceId);
        snapshot.setValue(value);
        snapshot.setLastReplyForClient(new HashMap<Long, Reply>());
        snapshot.setPartialResponseCache(new ArrayList<Reply>());
        catchUpSnapshot = new CatchUpSnapshot(view, requestTime, snapshot);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(view, catchUpSnapshot.getView());
        assertEquals(requestTime, catchUpSnapshot.getRequestTime());
        assertEquals(instanceId, catchUpSnapshot.getSnapshot().getNextInstanceId());
        assertArrayEquals(value, catchUpSnapshot.getSnapshot().getValue());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(catchUpSnapshot);

        byte[] bytes = catchUpSnapshot.toByteArray();
        assertEquals(bytes.length, catchUpSnapshot.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        CatchUpSnapshot deserializedCatchUpSnapshot = new CatchUpSnapshot(dis);

        assertEquals(MessageType.CatchUpSnapshot, type);
        compare(catchUpSnapshot, deserializedCatchUpSnapshot);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.CatchUpSnapshot, catchUpSnapshot.getType());
    }

    protected void compare(CatchUpSnapshot expected, CatchUpSnapshot actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());

        assertEquals(expected.getRequestTime(), actual.getRequestTime());
        assertEquals(expected.getSnapshot().getNextInstanceId(),
                actual.getSnapshot().getNextInstanceId());
        assertArrayEquals(expected.getSnapshot().getValue(), actual.getSnapshot().getValue());
    }
}
