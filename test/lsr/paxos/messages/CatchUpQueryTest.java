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
	private CatchUpQuery query;
	private int view;
	private int[] values;
	private Pair<Integer, Integer>[] ranges;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		view = 123;
		values = new int[] { 3, 5, 7, 13 };
		ranges = new Pair[] { new Pair<Integer, Integer>(1, 2), new Pair<Integer, Integer>(9, 12) };
		query = new CatchUpQuery(view, values, ranges);
	}

	@Test
	public void testDefaultConstructor() {
		assertEquals(view, query.getView());
		assertTrue(Arrays.equals(values, query.getInstanceIdArray()));
	}

	@Test
	public void testSerialization() throws IOException {
		byte[] bytes = query.toByteArray();
		assertEquals(bytes.length, query.byteSize());

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		MessageType type = MessageType.values()[dis.readByte()];
		CatchUpQuery deserializedQuery = new CatchUpQuery(dis);

		assertEquals(MessageType.CatchUpQuery, type);
		compare(query, deserializedQuery);
		assertEquals(0, dis.available());
	}

	@Test
	public void testCorrectMessageType() {
		assertEquals(MessageType.CatchUpQuery, query.getType());
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
