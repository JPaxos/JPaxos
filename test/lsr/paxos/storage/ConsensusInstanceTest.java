package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import lsr.paxos.storage.ConsensusInstance.LogEntryState;

import org.junit.Test;

public class ConsensusInstanceTest {
	@Test
	public void testSerialization() throws IOException {
		int instanceId = 3;
		int view = 4;
		LogEntryState state = LogEntryState.DECIDED;
		byte[] value = new byte[] { 12, 78, 90, 5, 4 };
		ConsensusInstance instance = new ConsensusInstance(instanceId, state, view, value);

		byte[] bytes = instance.toByteArray();

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);

		ConsensusInstance deserializedInstance = new ConsensusInstance(dis);

		assertEquals(instance, deserializedInstance);
		assertEquals(0, dis.available());
		assertEquals(bytes.length, instance.byteSize());
	}
}
