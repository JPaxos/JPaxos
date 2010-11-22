package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lsr.paxos.storage.ConsensusInstance;

import org.junit.Before;
import org.junit.Test;

public class CatchUpResponseTest {
	private int _view = 12;
	private long _requestTime = 574351;
	private CatchUpResponse _catchUpResponse;
	private List<ConsensusInstance> _instances;

	@Before
	public void setUp() {
		_instances = new ArrayList<ConsensusInstance>();
		_instances.add(new ConsensusInstance(0));
		_instances.get(0).setValue(4, new byte[] { 1, 2, 3 });
		_instances.add(new ConsensusInstance(1));
		_instances.get(0).setValue(5, new byte[] { 1, 4, 3 });
		_instances.add(new ConsensusInstance(2));
		_instances.get(0).setValue(6, new byte[] { 6, 9, 2 });

		_catchUpResponse = new CatchUpResponse(_view, _requestTime, _instances);
	}

	@Test
	public void testDefaultConstructor() {
		assertEquals(_view, _catchUpResponse.getView());
		assertEquals(_requestTime, _catchUpResponse.getRequestTime());
		assertEquals(_instances, _catchUpResponse.getDecided());
	}

	@Test
	public void testSerialization() throws IOException {
		byte[] bytes = _catchUpResponse.toByteArray();

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		MessageType type = MessageType.values()[dis.readByte()];
		CatchUpResponse deserializedPrepare = new CatchUpResponse(dis);

		assertEquals(MessageType.CatchUpResponse, type);
		compare(_catchUpResponse, deserializedPrepare);
		assertEquals(0, dis.available());
	}

	@Test
	public void testCorrectMessageType() {
		assertEquals(MessageType.CatchUpResponse, _catchUpResponse.getType());
	}

	private void compare(CatchUpResponse expected, CatchUpResponse actual) {
		assertEquals(expected.getView(), actual.getView());
		assertEquals(expected.getSentTime(), actual.getSentTime());
		assertEquals(expected.getType(), actual.getType());

		assertEquals(expected.getRequestTime(), actual.getRequestTime());
		assertEquals(expected.getDecided(), actual.getDecided());
	}
}
