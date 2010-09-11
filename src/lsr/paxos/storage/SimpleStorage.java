package lsr.paxos.storage;

import java.util.BitSet;
import java.util.List;
import java.util.SortedMap;

import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

//TODO comments
public class SimpleStorage implements Storage {
	/** How many proposals can be made concurrently */
	private int _windowSize;

	private int _firstUncommitted = 0;
	protected int _view = 0;
	private final StableStorage _stableStorage;
	private final ProcessDescriptor _process;
	private final BitSet _acceptors;
	private final BitSet _learners;

	/**
	 * Creates new SimpleStorage. It assumes that each process is acceptor and
	 * learner.
	 * 
	 * @param stableStorage
	 * @param p
	 */
	public SimpleStorage(StableStorage stableStorage, ProcessDescriptor p) {
		BitSet bs = new BitSet();
		_stableStorage = stableStorage;
		_process = p;
		bs.set(0, p.config.getN());
		_acceptors = bs;
		_learners = bs;
		_windowSize = p.windowSize;
	}

	public SimpleStorage(StableStorage stableStorage, ProcessDescriptor p,
			BitSet acceptors, BitSet learners) {
		_stableStorage = stableStorage;
		_process = p;
		_acceptors = acceptors;
		_learners = learners;
	}

	public int getFirstUncommitted() {
		return _firstUncommitted;
	}

	public int getView() {
		return _view;
	}

	public void setView(int view) throws IllegalArgumentException {
		if (view <= _view)
			throw new IllegalArgumentException(
					"Cannot set smaller or equal view.");
		_view = view;
	}

	public void updateFirstUncommitted() {
		if (_stableStorage.getLastSnapshot() != null)
			_firstUncommitted = Math.max(_firstUncommitted, 
			                             _stableStorage.getLastSnapshot().enclosingIntanceId+1);

		SortedMap<Integer, ConsensusInstance> log = 
			_stableStorage.getLog().getInstanceMap();
		while (_firstUncommitted < _stableStorage.getLog().getNextId()
				&& log.get(_firstUncommitted).getState() == LogEntryState.DECIDED) {
			_firstUncommitted++;
		}
	}

	public int getN() {
		return _process.config.getN();
	}

	public List<PID> getProcesses() {
		return _process.config.getProcesses();
	}

	public BitSet getAcceptors() {
		return (BitSet) _acceptors.clone();
	}

	public BitSet getLearners() {
		return (BitSet) _learners.clone();
	}

	public int getLocalId() {
		return _process.localID;
	}

	public StableStorage getStableStorage() {
		return _stableStorage;
	}

	public void setWindowSize(int maxActiveProposals) {
		maxActiveProposals = _windowSize;
	}

	public int getWindowSize() {
		return _windowSize;
	}

	public boolean isInWindow(int instanceId) {
		return instanceId < _firstUncommitted + _windowSize;
	}

	public Log getLog() {
		return _stableStorage.getLog();
	}
	
	/**
	 * @return true if there are no undecided consensus instances. 
	 */
	public boolean isIdle() {
		return  _stableStorage.getLog()._nextId == _firstUncommitted;
	}
}
