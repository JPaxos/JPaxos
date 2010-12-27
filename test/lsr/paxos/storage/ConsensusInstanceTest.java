package lsr.paxos.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

import org.junit.Test;

public class ConsensusInstanceTest {
    private int instanceId = 3;
    private int view = 4;
    private LogEntryState state = LogEntryState.DECIDED;
    private byte[] value = new byte[] {12, 78, 90, 5, 4};

    @Test
    public void shouldCreateInstance() {
        ConsensusInstance instance = new ConsensusInstance(instanceId, state, view, value);
        assertEquals(instanceId, instance.getId());
        assertEquals(state, instance.getState());
        assertEquals(view, instance.getView());
        assertEquals(value, instance.getValue());
    }

    @Test
    public void shouldCreateEmptyInstance() {
        ConsensusInstance instance = new ConsensusInstance(instanceId);
        assertEquals(instanceId, instance.getId());
        assertEquals(LogEntryState.UNKNOWN, instance.getState());
        assertEquals(-1, instance.getView());
        assertEquals(null, instance.getValue());
    }

    @Test
    public void testEqual() {
        ConsensusInstance instance = new ConsensusInstance(instanceId, state, view, value);
        ConsensusInstance equal = new ConsensusInstance(instanceId, state, view, value);
        ConsensusInstance differentId = new ConsensusInstance(5, state, view, value);
        ConsensusInstance differentView = new ConsensusInstance(instanceId, state, 5, value);
        ConsensusInstance differentValue = new ConsensusInstance(instanceId, state, view, null);
        ConsensusInstance differentState = new ConsensusInstance(instanceId, LogEntryState.KNOWN,
                view, value);

        assertFalse(instance.equals(new Object()));
        assertFalse(instance.equals(differentId));
        assertFalse(instance.equals(differentState));
        assertFalse(instance.equals(differentView));
        assertFalse(instance.equals(differentValue));
        assertTrue(instance.equals(equal));
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        serializeAndDeserialize(5, LogEntryState.UNKNOWN, 9, null);
        serializeAndDeserialize(5, LogEntryState.KNOWN, 9, new byte[] {1, 2, 3, 4});
        serializeAndDeserialize(5, LogEntryState.DECIDED, 9, new byte[] {1, 2, 3, 4});
    }

    @Test
    public void unknownInstanceWithValueShouldThrowException() {
        try {
            new ConsensusInstance(5, LogEntryState.UNKNOWN, 9, new byte[] {1, 2, 3, 4});
        } catch (Exception e) {
            return;
        }
        Assert.fail();
    }

    private void serializeAndDeserialize(int id, LogEntryState state, int view, byte[] value)
            throws IOException {
        ConsensusInstance instance = new ConsensusInstance(id, state, view, value);

        // Serialize using write and toByteArray methods.
        ByteBuffer buffer = ByteBuffer.allocate(instance.byteSize());
        instance.write(buffer);
        byte[] bytes = instance.toByteArray();

        assertEquals(0, buffer.remaining());
        assertArrayEquals(buffer.array(), bytes);
        assertEquals(bytes.length, instance.byteSize());

        // Deserialize consensus instance.
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes));
        ConsensusInstance deserializedInstance = new ConsensusInstance(stream);

        assertEquals(instance, deserializedInstance);
        assertEquals(0, stream.available());
    }
}
