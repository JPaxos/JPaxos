package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class InMemoryConsensusInstanceTest {
    private int instanceId = 3;
    private int view = 4;
    private LogEntryState state = LogEntryState.DECIDED;
    private byte[] value = new byte[] {12, 78, 90, 5, 4};

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
    }
    
    @Test
    public void shouldCreateInstance() {
        InMemoryConsensusInstance instance = new InMemoryConsensusInstance(instanceId, state, view,
                value);
        assertEquals(instanceId, instance.getId());
        assertEquals(state, instance.getState());
        assertEquals(view, instance.getLastVotedView());
        assertEquals(view, instance.getLastSeenView());
        assertEquals(value, instance.getValue());
    }

    @Test
    public void shouldCreateEmptyInstance() {
        InMemoryConsensusInstance instance = new InMemoryConsensusInstance(instanceId);
        assertEquals(instanceId, instance.getId());
        assertEquals(LogEntryState.UNKNOWN, instance.getState());
        assertEquals(-1, instance.getLastSeenView());
        assertEquals(-1, instance.getLastVotedView());
        assertEquals(null, instance.getValue());
    }

    @Test
    public void testEqual() {
        InMemoryConsensusInstance instance = new InMemoryConsensusInstance(instanceId, state, view,
                value);
        InMemoryConsensusInstance equal = new InMemoryConsensusInstance(instanceId, state, view,
                value);
        InMemoryConsensusInstance differentId = new InMemoryConsensusInstance(5, state, view,
                value);
        InMemoryConsensusInstance differentView = new InMemoryConsensusInstance(instanceId, state,
                5, value);
        InMemoryConsensusInstance differentValue = new InMemoryConsensusInstance(instanceId, state,
                view, new byte[] {12, 78, 90, 5, 5});
        InMemoryConsensusInstance differentState = new InMemoryConsensusInstance(instanceId,
                LogEntryState.KNOWN,
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

    /*
     * That's false
     * 
    @Test
    public void unknownInstanceWithValueShouldThrowException() {
        try {
            new InMemoryConsensusInstance(5, LogEntryState.UNKNOWN, 9, new byte[] {1, 2, 3, 4});
        } catch (RuntimeException e) {
            return;
        } catch (AssertionError e) {
            return;
        }
        assertEquals(0,1);
    }
     */

    private void serializeAndDeserialize(int id, LogEntryState state, int view, byte[] value)
            throws IOException {
        InMemoryConsensusInstance instance = new InMemoryConsensusInstance(id, state, view, value);

        // Serialize using write and toByteArray methods.
        ByteBuffer buffer = ByteBuffer.allocate(instance.byteSize());
        instance.writeAsLastVoted(buffer);

        assertEquals(0, buffer.remaining());
        buffer.flip();

        // Deserialize consensus instance.
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(buffer.array()));

        InMemoryConsensusInstance deserializedInstance = new InMemoryConsensusInstance(stream);

        assertEquals(instance, deserializedInstance);
        assertEquals(0, stream.available());
    }
}
