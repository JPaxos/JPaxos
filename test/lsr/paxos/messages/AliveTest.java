package lsr.paxos.messages;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.testng.annotations.Test;

@Test(groups = { "unit" })
public class AliveTest {
	private int _view = 12;
	private int _logSize = 32;
	private Alive _alive;

	public void setUp() {
		_alive = new Alive(_view, _logSize);
	}

	public void testDefaultConstructor() {
		assertEquals(_view, _alive.getView());
		assertEquals(_logSize, _alive.getLogSize());
	}

	public void testSerialization() throws IOException {
		byte[] bytes = _alive.toByteArray();
		assertEquals(bytes.length, _alive.byteSize());

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		MessageType type = MessageType.values()[dis.readByte()];
		Alive deserializedAlive = new Alive(dis);

		assertEquals(MessageType.Alive, type);

		compare(_alive, deserializedAlive);
		assertEquals(0, dis.available());
	}

	public void testCorrectMessageType() {
		assertEquals(MessageType.Alive, _alive.getType());
	}

	private void compare(Alive expected, Alive actual) {
		assertEquals(expected.getView(), actual.getView());
		assertEquals(expected.getSentTime(), actual.getSentTime());
		assertEquals(expected.getType(), actual.getType());

		assertEquals(expected.getLogSize(), actual.getLogSize());
	}
}
