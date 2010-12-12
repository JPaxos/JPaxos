package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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

public class InMemoryStorageTest {
    private Storage storage;
    private static int localId = 2;
    private List<PID> processes;

    @Before
    public void setUp() {
        processes = new ArrayList<PID>();
        processes.add(new PID(0, "replica0", 1000, 1001));
        processes.add(new PID(1, "replica1", 2000, 2001));
        processes.add(new PID(2, "replica2", 3000, 3001));
        ProcessDescriptor.initialize(new Configuration(processes), localId);

        storage = new InMemoryStorage();
    }

    @Test
    public void shouldReturnAcceptors() {
        BitSet acceptors = new BitSet();
        acceptors.set(0, 3);
        assertEquals(acceptors, storage.getAcceptors());
    }

    @Test
    public void shouldCopyAcceptorsBeforeReturn() {
        storage.getAcceptors().clear();

        BitSet acceptors = new BitSet();
        acceptors.set(0, 3);
        assertEquals(acceptors, storage.getAcceptors());
    }

    @Test
    public void shouldReturnLocalId() {
        assertEquals(localId, storage.getLocalId());
    }

    @Test
    public void testInitialValueOfFirstUncommitted() {
        assertEquals(0, storage.getFirstUncommitted());
    }

    @Test
    public void shouldReturnProcessesCount() {
        assertEquals(processes.size(), storage.getN());
    }

    @Test
    public void shouldUpdateFirstUncommited() {
        SortedMap<Integer, ConsensusInstance> map = new TreeMap<Integer, ConsensusInstance>();
        map.put(0, new ConsensusInstance(0, LogEntryState.DECIDED, 1, null));
        map.put(1, new ConsensusInstance(1, LogEntryState.DECIDED, 2, null));
        map.put(2, new ConsensusInstance(2, LogEntryState.KNOWN, 3, null));
        map.put(3, new ConsensusInstance(3, LogEntryState.DECIDED, 4, null));

        Log log = mock(Log.class);
        storage = new InMemoryStorage(log);
        when(log.getInstanceMap()).thenReturn(map);
        when(log.getNextId()).thenReturn(5);

        storage.updateFirstUncommitted();

        assertEquals(2, storage.getFirstUncommitted());
    }

    @Test
    public void testInitialViewNumber() {
        assertEquals(0, storage.getView());
    }

    @Test
    public void shouldSetHigherView() {
        storage.setView(5);
        assertEquals(5, storage.getView());

        storage.setView(9);
        assertEquals(9, storage.getView());
    }

    @Test
    public void shouldNotSetLowerView() {
        storage.setView(5);
        assertEquals(5, storage.getView());

        try {
            storage.setView(3);
        } catch (IllegalArgumentException e) {
            return;
        }
        fail();
    }

    @Test
    public void shouldNotSetEqualView() {
        storage.setView(5);
        assertEquals(5, storage.getView());
        try {
            storage.setView(5);
        } catch (IllegalArgumentException e) {
            return;
        }
        fail();
    }
}
