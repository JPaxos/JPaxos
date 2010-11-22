package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class AcceptTest {
	private Accept _accept;
	private int _view;
	private int _instanceId;
	private byte[] _values;

	@Before
	public void setUp() {
		_view = 123;
		_instanceId = 432;
		_values = new byte[] { 1, 5, 7, 3 };
		_accept = new Accept(_view, _instanceId);
	}

	@Test
	public void testDefaultConstructor() {
		assertEquals(_view, _accept.getView());
		assertEquals(_instanceId, _accept.getInstanceId());
	}

	@Test
	public void testAcceptFromProposeMessage() {
		Propose propose = new Propose(_view, _instanceId, _values);
		Accept accept = new Accept(propose);
		assertEquals(_accept, accept);
	}

	@Test
	public void testSerialization() throws IOException {
		byte[] bytes = _accept.toByteArray();

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		MessageType type = MessageType.values()[dis.readByte()];
		Accept deserializedAccept = new Accept(dis);

		assertEquals(MessageType.Accept, type);
		assertEquals(_accept, deserializedAccept);
		assertEquals(0, dis.available());
		assertEquals(bytes.length, _accept.byteSize());
	}

	@Test
	public void testCorrectMessageType() {
		assertEquals(MessageType.Accept, _accept.getType());
	}
}
