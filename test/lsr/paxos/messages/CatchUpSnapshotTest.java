package lsr.paxos.messages;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import lsr.common.Pair;

import org.testng.annotations.Test;

@Test(groups = { "unit" })
public class CatchUpSnapshotTest {
	private int _view = 12;
	private long _requestTime = 32485729;
	private byte[] _value = new byte[] { 1, 7, 4, 5 };
	private int _instanceId = 52;
	private Pair<Integer, byte[]> _snapshot;
	private CatchUpSnapshot _catchUpSnapshot;

	public void setUp() {
		_snapshot = new Pair<Integer, byte[]>(_instanceId, _value);
		_catchUpSnapshot = new CatchUpSnapshot(_view, _requestTime, _snapshot);
	}

	public void testDefaultConstructor() {
		assertEquals(_view, _catchUpSnapshot.getView());
		assertEquals(_requestTime, _catchUpSnapshot.getRequestTime());
		assertEquals(new Integer(_instanceId), _catchUpSnapshot.getSnapshot().getKey());
		assertTrue(Arrays.equals(_value, _catchUpSnapshot.getSnapshot().getValue()));
	}

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

	public void testCorrectMessageType() {
		assertEquals(MessageType.CatchUpSnapshot, _catchUpSnapshot.getType());
	}

	private void compare(CatchUpSnapshot expected, CatchUpSnapshot actual) {
		assertEquals(expected.getView(), actual.getView());
		assertEquals(expected.getSentTime(), actual.getSentTime());
		assertEquals(expected.getType(), actual.getType());

		assertEquals(expected.getRequestTime(), actual.getRequestTime());
		assertEquals(expected.getSnapshot().getKey(), actual.getSnapshot().getKey());
		assertTrue(Arrays.equals(expected.getSnapshot().getValue(), actual.getSnapshot().getValue()));
	}
}
