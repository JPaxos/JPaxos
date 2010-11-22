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
	private Storage _storage;
	private StableStorage _stableStorage;
	private static int _localId = 2;
	private BitSet _acceptors;
	private BitSet _learners;
	private List<PID> _processes;

	@Before
	public void setUp() {
		_stableStorage = mock(StableStorage.class);

		_processes = new ArrayList<PID>();
		_processes.add(new PID(0, "replica0", 1000, 1001));
		_processes.add(new PID(1, "replica1", 2000, 2001));
		_processes.add(new PID(2, "replica2", 3000, 3001));

		ProcessDescriptor.initialize(new Configuration(_processes), _localId);
		
		_acceptors = new BitSet();
		_acceptors.set(1, 3);
		_learners = new BitSet();
		_learners.set(0, 2);

		_storage = new SimpleStorage(_stableStorage, _acceptors, _learners);
	}

	@Test
	public void testStableStorageGetter() {
		assertEquals(_stableStorage, _storage.getStableStorage());
	}

	@Test
	public void testLearnersGetter() {
		assertEquals(_learners, _storage.getLearners());
	}

	@Test
	public void testAcceptorGetter() {
		assertEquals(_acceptors, _storage.getAcceptors());
	}

	@Test
	public void testLocalIdGetter() {
		assertEquals(_localId, _storage.getLocalId());
	}

	@Test
	public void testInitialFirstUncommitted() {
		assertEquals(0, _storage.getFirstUncommitted());
	}

	@Test
	public void testProcessesGetter() {
		assertEquals(_processes, _storage.getProcesses());
	}

	@Test
	public void testNGetter() {
		assertEquals(_processes.size(), _storage.getN());
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
		when(_stableStorage.getLog()).thenReturn(log);

		_storage.updateFirstUncommitted();

		assertEquals(2, _storage.getFirstUncommitted());
	}
}
