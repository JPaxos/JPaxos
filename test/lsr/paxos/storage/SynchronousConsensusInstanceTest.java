package lsr.paxos.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class SynchronousConsensusInstanceTest {
    private DiscWriter writer;
    private SynchronousConsensusInstace instance;
    private int view;
    private byte[] values;

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
        writer = mock(DiscWriter.class);
        instance = new SynchronousConsensusInstace(2, writer);

        view = 1;
        values = new byte[] {1, 2, 3};
    }

    @Test
    public void shouldInitializeFromConsensusInstance() {
        ConsensusInstance consensusInstance = new InMemoryConsensusInstance(1, LogEntryState.KNOWN,
                3,
                values);

        instance = new SynchronousConsensusInstace(consensusInstance, writer);
        assertEquals(1, instance.getId());
        assertEquals(LogEntryState.KNOWN, instance.getState());
        assertEquals(3, instance.getLastSeenView());
        assertArrayEquals(values, instance.getValue());
    }

    @Test
    public void shouldInitialize() {
        instance = new SynchronousConsensusInstace(1, LogEntryState.KNOWN, 3, values, writer);
        assertEquals(1, instance.getId());
        assertEquals(LogEntryState.KNOWN, instance.getState());
        assertEquals(3, instance.getLastSeenView());
        assertArrayEquals(values, instance.getValue());
    }

    @Test
    public void shouldChangeValueOnEmptyInstance() {
        instance.updateStateFromPropose(0, view, values);
        verify(writer).changeInstanceValue(2, view, values);
    }

    @Test
    public void shouldWriteTheValue() {
        instance = new SynchronousConsensusInstace(1, LogEntryState.UNKNOWN, 3, null, writer);
        instance.updateStateFromDecision(3, values);
        verify(writer, times(1)).changeInstanceValue(1, 3, values);
    }

    @Test
    public void shouldWriteDecided() {
        instance.updateStateFromDecision(view, values);
        instance.setDecided();

        verify(writer).decideInstance(2);
        assertEquals(LogEntryState.DECIDED, instance.getState());
    }

    /*
     * FIXME : the idea of this test is good, but calling protected methods is bad
     *
    @Test
    public void shouldSetValueToUnknown() {
        instance.setValue(view, values);
        instance.setValue(view + 1, null);
        assertEquals(LogEntryState.UNKNOWN, instance.getState());
    }
    */

    @Test
    public void shouldThrowExceptionAfterSettingValueWithOldView() {
        instance.updateStateFromPropose(2, 5, new byte[] {1, 2, 3});
        try{
            instance.updateStateFromPropose(0, 3, new byte[] {1, 2, 3});
        } catch(RuntimeException e) {
            return;
        } catch(AssertionError e) {
            return;
        }
        fail();
    }
}
