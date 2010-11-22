package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class ProposeTest {
	private int _view = 12;
	private int _instanceId = 23;
	private byte[] _value = new byte[] { 1, 7, 4, 5 };
	private Propose _propose;

	@Before
	public void setUp() {
		_propose = new Propose(_view, _instanceId, _value);
	}

	@Test
	public void testDefaultConstructor() {
		assertEquals(_view, _propose.getView());
		assertEquals(_instanceId, _propose.getInstanceId());
		assertTrue(Arrays.equals(_value, _propose.getValue()));
	}

	@Test
	public void testSerialization() throws IOException {
		byte[] bytes = _propose.toByteArray();
		assertEquals(bytes.length, _propose.byteSize());

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		MessageType type = MessageType.values()[dis.readByte()];
		Propose deserializedPropose = new Propose(dis);

		assertEquals(MessageType.Propose, type);
		compare(_propose, deserializedPropose);
		assertEquals(0, dis.available());
	}

	@Test
	public void testCorrectMessageType() {
		assertEquals(MessageType.Propose, _propose.getType());
	}

	private void compare(Propose expected, Propose actual) {
		assertEquals(expected.getView(), actual.getView());
		assertEquals(expected.getSentTime(), actual.getSentTime());
		assertEquals(expected.getType(), actual.getType());

		assertEquals(expected.getInstanceId(), actual.getInstanceId());
		assertTrue(Arrays.equals(expected.getValue(), actual.getValue()));
	}
}
