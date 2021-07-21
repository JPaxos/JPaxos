package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import lsr.common.ProcessDescriptorHelper;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class SynchronousLogTest {

    @Before
    public void setUp() {
        ProcessDescriptorHelper.initialize(3, 0);
    }

    @Test
    public void testAppend() throws IOException {
        DiscWriter writer = mock(DiscWriter.class);
        SynchronousLog log = new SynchronousLog(writer);

        int view = 1;
        byte[] value = new byte[] {1, 2, 3};
        ConsensusInstance instance = log.append();
        instance.updateStateFromDecision(view, value);
        verify(writer).changeInstanceValue(0, view, value);
        assertTrue(instance instanceof SynchronousConsensusInstace);
    }

    @Test
    public void testCreatesSynchronousConsensusInstance() throws IOException {
        DiscWriter writer = mock(DiscWriter.class);
        SynchronousLog log = new SynchronousLog(writer);

        ConsensusInstance instance = log.getInstance(0);
        assertTrue(instance instanceof SynchronousConsensusInstace);
    }

    @Test
    public void testInitialInstances() throws IOException {
        DiscWriter writer = mock(DiscWriter.class);
        ConsensusInstance instance1 = new SynchronousConsensusInstace(1, LogEntryState.KNOWN, 1,
                new byte[] {1}, writer);
        ConsensusInstance instance2 = new SynchronousConsensusInstace(2, LogEntryState.KNOWN, 1,
                new byte[] {1}, writer);
        Collection<ConsensusInstance> instances = new ArrayList<ConsensusInstance>();
        instances.add(instance1);
        instances.add(instance2);
        
        when(writer.load()).thenReturn(instances);
        SynchronousLog log = new SynchronousLog(writer);

        areEquals(log.getInstance(1), instance1);
        areEquals(log.getInstance(2), instance2);
        assertTrue(log.getInstance(1) instanceof SynchronousConsensusInstace);
        assertTrue(log.getInstance(2) instanceof SynchronousConsensusInstace);
    }

    @Test
    public void testNotSortedInitialInstances() throws IOException {
        DiscWriter writer = mock(DiscWriter.class);
        
        ConsensusInstance instance1 = new SynchronousConsensusInstace(1, LogEntryState.KNOWN, 1,
                new byte[] {1}, writer);
        ConsensusInstance instance2 = new SynchronousConsensusInstace(2, LogEntryState.KNOWN, 1,
                new byte[] {1}, writer);
        Collection<ConsensusInstance> instances = new ArrayList<ConsensusInstance>();
        instances.add(instance2);
        instances.add(instance1);

        when(writer.load()).thenReturn(instances);
        SynchronousLog log = new SynchronousLog(writer);

        areEquals(log.getInstance(1), instance1);
        areEquals(log.getInstance(2), instance2);
        assertTrue(log.getInstance(1) instanceof SynchronousConsensusInstace);
        assertTrue(log.getInstance(2) instanceof SynchronousConsensusInstace);
    }

    private void areEquals(ConsensusInstance actual, ConsensusInstance expected) {
        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getState(), expected.getState());
        assertEquals(actual.getLastVotedView(), expected.getLastVotedView());
        assertEquals(actual.getLastSeenView(), expected.getLastSeenView());
        assertEquals(actual.getValue(), expected.getValue());
    }
}
