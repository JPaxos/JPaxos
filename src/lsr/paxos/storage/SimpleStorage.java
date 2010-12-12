package lsr.paxos.storage;

import java.util.BitSet;
import java.util.SortedMap;

import lsr.common.ProcessDescriptor;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * Simple implementation of <code>Storage</code> interface.
 */
public class SimpleStorage implements Storage {

    private int firstUncommitted = 0;
    private final StableStorage stableStorage;
    private final BitSet acceptors;

    /**
     * Creates new instance of <code>SimpleStorage</code>. It assumes that each
     * process is acceptor.
     * 
     * @param stableStorage
     */
    public SimpleStorage(StableStorage stableStorage) {
        this.stableStorage = stableStorage;
        acceptors = new BitSet();
        acceptors.set(0, getN());
    }

    public int getFirstUncommitted() {
        return firstUncommitted;
    }

    public void updateFirstUncommitted() {
        if (stableStorage.getLastSnapshot() != null)
            firstUncommitted = Math.max(firstUncommitted,
                    stableStorage.getLastSnapshot().nextIntanceId);

        SortedMap<Integer, ConsensusInstance> log = stableStorage.getLog().getInstanceMap();
        while (firstUncommitted < stableStorage.getLog().getNextId() &&
               log.get(firstUncommitted).getState() == LogEntryState.DECIDED) {
            firstUncommitted++;
        }
    }

    public int getN() {
        return ProcessDescriptor.getInstance().config.getN();
    }

    public BitSet getAcceptors() {
        return (BitSet) acceptors.clone();
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

    public boolean isIdle() {
        return stableStorage.getLog().nextId == firstUncommitted;
    }
}
