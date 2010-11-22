package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import lsr.common.Pair;

import org.junit.Before;
import org.junit.Test;

public class CatchUpQueryTest {
	private CatchUpQuery _query;
	private int _view;
	private int[] _values;
	private Pair<Integer, Integer>[] _ranges;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		_view = 123;
		_values = new int[] { 3, 5, 7, 13 };
		_ranges = new Pair[] { new Pair<Integer, Integer>(1, 2), new Pair<Integer, Integer>(9, 12) };
		_query = new CatchUpQuery(_view, _values, _ranges);
	}

	@Test
	public void testDefaultConstructor() {
		assertEquals(_view, _query.getView());
		assertTrue(Arrays.equals(_values, _query.getInstanceIdArray()));
	}

	@Test
	public void testSerialization() throws IOException {
		byte[] bytes = _query.toByteArray();
		assertEquals(bytes.length, _query.byteSize());

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		MessageType type = MessageType.values()[dis.readByte()];
		CatchUpQuery deserializedQuery = new CatchUpQuery(dis);

		assertEquals(MessageType.CatchUpQuery, type);
		compare(_query, deserializedQuery);
		assertEquals(0, dis.available());
	}

	@Test
	public void testCorrectMessageType() {
		assertEquals(MessageType.CatchUpQuery, _query.getType());
	}

	private void compare(CatchUpQuery expected, CatchUpQuery actual) {
		assertEquals(expected.getView(), actual.getView());
		assertEquals(expected.getSentTime(), actual.getSentTime());
		assertEquals(expected.getType(), actual.getType());

		assertTrue(Arrays.equals(expected.getInstanceIdArray(), actual.getInstanceIdArray()));
		assertTrue(Arrays.equals(expected.getInstanceIdRangeArray(), actual.getInstanceIdRangeArray()));
		assertEquals(expected.isPeriodicQuery(), actual.isPeriodicQuery());
		assertEquals(expected.isSnapshotRequest(), actual.isSnapshotRequest());
	}
}
