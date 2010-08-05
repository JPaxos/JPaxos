package lsr.paxos.messages;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test(groups = { "unit" })
public class AcceptTest {
	private Accept _accept;
	private int _view;
	private int _instanceId;
	private byte[] _values;

	public void setUp() {
		_view = 123;
		_instanceId = 432;
		_values = new byte[] { 1, 5, 7, 3 };
		_accept = new Accept(_view, _instanceId, _values);
	}

	public void testDefaultConstructor() {
		assertEquals(_view, _accept.getView());
		assertEquals(_instanceId, _accept.getInstanceId());
		assertTrue(Arrays.equals(_values, _accept.getValue()));
	}

	public void testAcceptFromProposeMessage() {
		Propose propose = new Propose(_view, _instanceId, _values);
		Accept accept = new Accept(propose);
		assertEquals(_accept, accept);
	}

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

	public void testCorrectMessageType() {
		assertEquals(MessageType.Accept, _accept.getType());
	}
}
