package lsr.paxos.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import lsr.common.Configuration;
import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.Snapshot;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class InMemoryStorageTest {
    private Storage storage;

    @Before
    public void setUp() {
        List<PID> processes = new ArrayList<PID>();
        processes.add(mock(PID.class));
        processes.add(mock(PID.class));
        processes.add(mock(PID.class));
        Configuration configuration = new Configuration(processes);
        ProcessDescriptor.initialize(configuration, 0);

        storage = new InMemoryStorage();
    }

    @Test
    public void initialValueOfFirstUncommittedShouldEqualZero() {
        assertEquals(0, storage.getFirstUncommitted());
    }

    @Test
    public void shouldCreateEmptyLog() {
        assertEquals(0, storage.getLog().getInstanceMap().size());
    }

    @Test
    public void shouldUpdateFirstUncommited() {
        final SortedMap<Integer, ConsensusInstance> map = new TreeMap<Integer, ConsensusInstance>();
        map.put(0, new InMemoryConsensusInstance(0, LogEntryState.DECIDED, 1, new byte[] {11}));
        map.put(1, new InMemoryConsensusInstance(1, LogEntryState.DECIDED, 2, new byte[] {22}));
        map.put(2, new InMemoryConsensusInstance(2, LogEntryState.KNOWN, 3, new byte[] {33}));
        map.put(3, new InMemoryConsensusInstance(3, LogEntryState.DECIDED, 4, new byte[] {44}));

        
        final Log logR = new InMemoryLog() {
            @Override
            public SortedMap<Integer, ConsensusInstance> getInstanceMap() {
                return map;
            }   
        };
        
        Log log = mock(logR.getClass());
        storage = new InMemoryStorage(log);
        when(log.getInstanceMap()).thenCallRealMethod();
        when(log.getNextId()).thenReturn(5);

        storage.updateFirstUncommitted();

        assertEquals(2, storage.getFirstUncommitted());
    }

    @Test
    public void shouldUpdateFirstUncommitedWithSnapshot() {
        Snapshot snapshot = new Snapshot();
        snapshot.setNextInstanceId(5);

        storage.setLastSnapshot(snapshot);
        storage.updateFirstUncommitted();

        assertEquals(5, storage.getFirstUncommitted());
    }

    @Test
    public void initialViewNumberShouldEqualZero() {
        assertEquals(0, storage.getView());
    }

    @Test
    public void shouldSetHigherView() {
        storage.setView(5);
        assertEquals(5, storage.getView());

        storage.setView(9);
        assertEquals(9, storage.getView());
    }
    
    /*
     * There's only an assert for this, and so it be
    @Test
    public void shouldNotSetLowerView() {
        storage.setView(5);
        assertEquals(5, storage.getView());

        try {
            storage.setView(3);
        } catch (IllegalArgumentException e) {
            return;
        } catch (AssertionError e) {
            return;
        }
        fail();
    }
     */

    /*
     * There's only an assert for this, and so it be
    @Test
    public void shouldNotSetEqualView() {
        storage.setView(5);
        assertEquals(5, storage.getView());
        try {
            storage.setView(5);
        } catch (IllegalArgumentException e) {
            return;
        } catch (AssertionError e) {
            return;
        }
        fail();
    }
     */

    @Test
    public void shouldHaveEmptyEpochAfterInitialization() {
        assertEquals(0, storage.getEpoch().length);
    }

    @Test
    public void shouldAllowToSetEpoch() {
        long[] epoch = new long[] {3, 1, 2};
        storage.setEpoch(epoch);
        assertArrayEquals(epoch, storage.getEpoch());
    }

    @Test
    public void shouldUpdateEpoch() {
        storage.setEpoch(new long[] {3, 1, 2});
        storage.updateEpoch(new long[] {2, 2, 3});
        assertArrayEquals(new long[] {3, 2, 3}, storage.getEpoch());
    }

    @Test
    public void shouldUpdateSingleEpoch() {
        storage.setEpoch(new long[] {1, 2, 3});
        storage.updateEpoch(10, 1);

        assertArrayEquals(new long[] {1, 10, 3}, storage.getEpoch());
    }

    @Test
    public void shouldThrowExceptionForInvalidEpochVector() {
        storage.setEpoch(new long[] {1, 2, 3});
        try {
            storage.updateEpoch(new long[] {1, 2, 3, 4});
        } catch (ArrayIndexOutOfBoundsException e) {
            return;
        } catch (AssertionError e) {
            return;
        }
        fail();
    }

    @Test
    public void shouldThrowExceptionForInvalidEpoch() {
        storage.setEpoch(new long[] {1, 2, 3});
        try {
            storage.updateEpoch(5, 3);
        } catch (ArrayIndexOutOfBoundsException e) {
            return;
        } catch (AssertionError e) {
            return;
        }
        fail();
    }

    @Test
    public void shouldNotHaveSnapshotAfterInitialization() {
        assertNull(storage.getLastSnapshot());
    }

    @Test
    public void shouldSetNewSnapshot() {
        Snapshot snapshot = mock(Snapshot.class);
        storage.setLastSnapshot(snapshot);
        assertEquals(snapshot, storage.getLastSnapshot());
    }
}
