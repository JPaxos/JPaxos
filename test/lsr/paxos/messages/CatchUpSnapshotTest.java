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
	private int _view = 12;
	private long _requestTime = 32485729;
	private byte[] _value = new byte[] { 1, 7, 4, 5 };
	private int _instanceId = 52;
	private Snapshot _snapshot;
	private CatchUpSnapshot _catchUpSnapshot;

	@Before
	public void setUp() {
		_snapshot = new Snapshot();
		_snapshot.nextIntanceId = _instanceId;
		_snapshot.value = _value;
		_snapshot.lastReplyForClient = new HashMap<Long, Reply>();
		_snapshot.partialResponseCache = new ArrayList<Reply>();
		_catchUpSnapshot = new CatchUpSnapshot(_view, _requestTime, _snapshot);
	}

	@Test
	public void testDefaultConstructor() {
		assertEquals(_view, _catchUpSnapshot.getView());
		assertEquals(_requestTime, _catchUpSnapshot.getRequestTime());
		assertEquals(new Integer(_instanceId), _catchUpSnapshot.getSnapshot().nextIntanceId);
		assertTrue(Arrays.equals(_value, _catchUpSnapshot.getSnapshot().value));
	}

	@Test
	public void testSerialization() throws IOException {
		byte[] bytes = _catchUpSnapshot.toByteArray();
		assertEquals(bytes.length, _catchUpSnapshot.byteSize());

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		MessageType type = MessageType.values()[dis.readByte()];
		CatchUpSnapshot deserializedCatchUpSnapshot = new CatchUpSnapshot(dis);

		assertEquals(MessageType.CatchUpSnapshot, type);
		compare(_catchUpSnapshot, deserializedCatchUpSnapshot);
		assertEquals(0, dis.available());
	}

	@Test
	public void testCorrectMessageType() {
		assertEquals(MessageType.CatchUpSnapshot, _catchUpSnapshot.getType());
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
