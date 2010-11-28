package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import lsr.common.Configuration;
import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

import org.junit.Before;
import org.junit.Test;

public class SimpleStorageTest {
    private Storage storage;
    private StableStorage stableStorage;
    private static int localId = 2;
    private BitSet acceptors;
    private BitSet learners;
    private List<PID> processes;

    @Before
    public void setUp() {
        stableStorage = mock(StableStorage.class);

        processes = new ArrayList<PID>();
        processes.add(new PID(0, "replica0", 1000, 1001));
        processes.add(new PID(1, "replica1", 2000, 2001));
        processes.add(new PID(2, "replica2", 3000, 3001));

        ProcessDescriptor.initialize(new Configuration(processes), localId);

        acceptors = new BitSet();
        acceptors.set(1, 3);
        learners = new BitSet();
        learners.set(0, 2);

        storage = new SimpleStorage(stableStorage, acceptors, learners);
    }

    @Test
    public void testStableStorageGetter() {
        assertEquals(stableStorage, storage.getStableStorage());
    }

    @Test
    public void testLearnersGetter() {
        assertEquals(learners, storage.getLearners());
    }

    @Test
    public void testAcceptorGetter() {
        assertEquals(acceptors, storage.getAcceptors());
    }

    @Test
    public void testLocalIdGetter() {
        assertEquals(localId, storage.getLocalId());
    }

    @Test
    public void testInitialFirstUncommitted() {
        assertEquals(0, storage.getFirstUncommitted());
    }

    @Test
    public void testProcessesGetter() {
        assertEquals(processes, storage.getProcesses());
    }

    @Test
    public void testNGetter() {
        assertEquals(processes.size(), storage.getN());
    }

    @Test
    public void testUpdateFirstUncommited() {
        SortedMap<Integer, ConsensusInstance> map = new TreeMap<Integer, ConsensusInstance>();
        map.put(0, new ConsensusInstance(0, LogEntryState.DECIDED, 1, null));
        map.put(1, new ConsensusInstance(1, LogEntryState.DECIDED, 2, null));
        map.put(2, new ConsensusInstance(2, LogEntryState.KNOWN, 3, null));
        map.put(3, new ConsensusInstance(3, LogEntryState.DECIDED, 4, null));

        Log log = mock(Log.class);
        when(log.getInstanceMap()).thenReturn(map);
        when(log.getNextId()).thenReturn(5);
        when(stableStorage.getLog()).thenReturn(log);

        storage.updateFirstUncommitted();

        assertEquals(2, storage.getFirstUncommitted());
    }
}
