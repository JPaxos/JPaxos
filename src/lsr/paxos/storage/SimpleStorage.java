package lsr.paxos.storage;

import java.util.BitSet;
import java.util.List;
import java.util.SortedMap;

import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

//TODO comments
public class SimpleStorage implements Storage {

	private int firstUncommitted = 0;
	protected int view = 0;
	private final StableStorage stableStorage;
	private final BitSet acceptors;
	private final BitSet learners;

	/**
	 * Creates new SimpleStorage. It assumes that each process is acceptor and
	 * learner.
	 * 
	 * @param stableStorage
	 * @param p
	 */
	public SimpleStorage(StableStorage stableStorage) {
		BitSet bs = new BitSet();
		this.stableStorage = stableStorage;		
		bs.set(0, ProcessDescriptor.getInstance().config.getN());
		acceptors = bs;
		learners = bs;
	}

	public SimpleStorage(StableStorage stableStorage, BitSet acceptors, BitSet learners) {
		this.stableStorage = stableStorage;
		this.acceptors = acceptors;
		this.learners = learners;
	}

	public int getFirstUncommitted() {
		return firstUncommitted;
	}

	public int getView() {
		return view;
	}

	public void setView(int view) throws IllegalArgumentException {
		if (view <= this.view)
			throw new IllegalArgumentException(
					"Cannot set smaller or equal view.");
		this.view = view;
	}

	public void updateFirstUncommitted() {
		if (stableStorage.getLastSnapshot() != null)
			firstUncommitted = Math.max(firstUncommitted, 
			                             stableStorage.getLastSnapshot().nextIntanceId);

		SortedMap<Integer, ConsensusInstance> log = 
			stableStorage.getLog().getInstanceMap();
		while (firstUncommitted < stableStorage.getLog().getNextId()
				&& log.get(firstUncommitted).getState() == LogEntryState.DECIDED) {
			firstUncommitted++;
		}
	}

	public int getN() {
		return ProcessDescriptor.getInstance().config.getN();
	}

	public List<PID> getProcesses() {
		return ProcessDescriptor.getInstance().config.getProcesses();
	}

	public BitSet getAcceptors() {
		return (BitSet) acceptors.clone();
	}

	public BitSet getLearners() {
		return (BitSet) learners.clone();
	}

	public int getLocalId() {
		return ProcessDescriptor.getInstance().localID;
	}

	public StableStorage getStableStorage() {
		return stableStorage;
	}

	public boolean isInWindow(int instanceId) {
		return instanceId < firstUncommitted + ProcessDescriptor.getInstance().windowSize;
	}

	public Log getLog() {
		return stableStorage.getLog();
	}
	
	/**
	 * @return true if there are no undecided consensus instances. 
	 */
	public boolean isIdle() {
		return  stableStorage.getLog().nextId == firstUncommitted;
	}
}
