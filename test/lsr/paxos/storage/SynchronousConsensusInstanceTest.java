package lsr.paxos.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

import org.junit.Before;
import org.junit.Test;

public class SynchronousConsensusInstanceTest {
    private DiscWriter writer;
    private SynchronousConsensusInstace instance;
    private int view;
    private byte[] values;

    @Before
    public void setUp() {
        writer = mock(DiscWriter.class);
        instance = new SynchronousConsensusInstace(2, writer);

        view = 1;
        values = new byte[] {1, 2, 3};
    }

    @Test
    public void shouldInitializeFromConsensusInstance() {
        ConsensusInstance consensusInstance = new ConsensusInstance(1, LogEntryState.KNOWN, 3,
                values);

        instance = new SynchronousConsensusInstace(consensusInstance, writer);
        assertEquals(1, instance.getId());
        assertEquals(LogEntryState.KNOWN, instance.getState());
        assertEquals(3, instance.getView());
        assertArrayEquals(values, instance.getValue());
    }

    @Test
    public void shouldInitialize() {
        instance = new SynchronousConsensusInstace(1, LogEntryState.KNOWN, 3, values, writer);
        assertEquals(1, instance.getId());
        assertEquals(LogEntryState.KNOWN, instance.getState());
        assertEquals(3, instance.getView());
        assertArrayEquals(values, instance.getValue());
    }

    @Test
    public void shouldChangeValueOnEmptyInstance() {
        instance.setValue(view, values);
        verify(writer).changeInstanceValue(2, view, values);
    }

    @Test
    public void shouldChangeViewOnEmptyInstance() {
        instance.setView(view);
        verify(writer).changeInstanceView(2, view);
    }

    @Test
    public void shouldNotWriteToDiscWhenSettingTheSameViewTwice() {
        instance.setView(view);
        instance.setView(view);
        verify(writer, times(1)).changeInstanceView(2, view);
    }

    @Test
    public void shouldWriteJustViewAfterSetingTheSameValueTwice() {
        instance.setValue(view, values);
        instance.setValue(view + 1, values);
        verify(writer, times(1)).changeInstanceValue(2, view, values);
        verify(writer, times(1)).changeInstanceView(2, view + 1);
    }

    @Test
    public void shouldWriteTheValue() {
        instance = new SynchronousConsensusInstace(1, LogEntryState.KNOWN, 3, null, writer);
        instance.setValue(3, values);
        verify(writer, times(1)).changeInstanceValue(1, 3, values);
    }

    @Test
    public void shouldWriteDecided() {
        instance.setValue(view, values);
        instance.setDecided();

        verify(writer).decideInstance(2);
        assertEquals(LogEntryState.DECIDED, instance.getState());
    }

    @Test
    public void shouldSetValueToUnknown() {
        instance.setValue(view, values);
        instance.setValue(view + 1, null);
        assertEquals(LogEntryState.UNKNOWN, instance.getState());
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionAfterSettingValueWithOldView() {
        instance.setValue(5, new byte[] {1, 2, 3});
        instance.setValue(3, new byte[] {1, 2, 3});
    }
}
