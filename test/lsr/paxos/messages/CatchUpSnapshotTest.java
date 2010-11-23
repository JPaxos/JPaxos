package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import lsr.common.Reply;
import lsr.paxos.Snapshot;

import org.junit.Before;
import org.junit.Test;

public class CatchUpSnapshotTest {
	private int view = 12;
	private long requestTime = 32485729;
	private byte[] value = new byte[] { 1, 7, 4, 5 };
	private int instanceId = 52;
	private Snapshot snapshot;
	private CatchUpSnapshot catchUpSnapshot;

	@Before
	public void setUp() {
		snapshot = new Snapshot();
		snapshot.nextIntanceId = instanceId;
		snapshot.value = value;
		snapshot.lastReplyForClient = new HashMap<Long, Reply>();
		snapshot.partialResponseCache = new ArrayList<Reply>();
		catchUpSnapshot = new CatchUpSnapshot(view, requestTime, snapshot);
	}

	@Test
	public void testDefaultConstructor() {
		assertEquals(view, catchUpSnapshot.getView());
		assertEquals(requestTime, catchUpSnapshot.getRequestTime());
		assertEquals(new Integer(instanceId), catchUpSnapshot.getSnapshot().nextIntanceId);
		assertTrue(Arrays.equals(value, catchUpSnapshot.getSnapshot().value));
	}

	@Test
	public void testSerialization() throws IOException {
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
	public void testCorrectMessageType() {
		assertEquals(MessageType.CatchUpSnapshot, catchUpSnapshot.getType());
	}

	private void compare(CatchUpSnapshot expected, CatchUpSnapshot actual) {
		assertEquals(expected.getView(), actual.getView());
		assertEquals(expected.getSentTime(), actual.getSentTime());
		assertEquals(expected.getType(), actual.getType());

		assertEquals(expected.getRequestTime(), actual.getRequestTime());
		assertEquals(expected.getSnapshot().nextIntanceId, actual.getSnapshot().nextIntanceId);
		assertTrue(Arrays.equals(expected.getSnapshot().value, actual.getSnapshot().value));
	}
}
