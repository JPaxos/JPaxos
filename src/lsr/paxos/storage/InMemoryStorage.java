package lsr.paxos.storage;

import java.util.BitSet;
import java.util.SortedMap;

import lsr.common.ProcessDescriptor;
import lsr.paxos.Snapshot;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class InMemoryStorage implements Storage {
    protected int view;
    protected Log log;
    private Snapshot lastSnapshot;
    private int firstUncommitted = 0;

    /**
     * Initializes new instance of <code>InMemoryStorage</code> class with empty
     * log.
     */
    public InMemoryStorage() {
        log = new Log();
    }

    /**
     * Initializes new instance of <code>InMemoryStorage</code> class with
     * specified log.
     * 
     * @param log - the initial content of the log
     */
    public InMemoryStorage(Log log) {
        this.log = log;
    }

    public Log getLog() {
        return log;
    }

    public Snapshot getLastSnapshot() {
        return lastSnapshot;
    }

    public void setLastSnapshot(Snapshot snapshot) {
        assert lastSnapshot == null || lastSnapshot.compare(snapshot) <= 0;
        lastSnapshot = snapshot;
    }

    public int getView() {
        return view;
    }

    public void setView(int view) throws IllegalArgumentException {
        if (view <= this.view)
            throw new IllegalArgumentException("Cannot set smaller or equal view.");
        this.view = view;
    }

    public int getFirstUncommitted() {
        return firstUncommitted;
    }

    public void updateFirstUncommitted() {
        if (lastSnapshot != null)
            firstUncommitted = Math.max(firstUncommitted,
                    lastSnapshot.nextIntanceId);

        SortedMap<Integer, ConsensusInstance> logs = log.getInstanceMap();
        while (firstUncommitted < log.getNextId() &&
               logs.get(firstUncommitted).getState() == LogEntryState.DECIDED) {
            firstUncommitted++;
        }
    }

    public int getN() {
        return ProcessDescriptor.getInstance().config.getN();
    }

    public BitSet getAcceptors() {
        BitSet acceptors = new BitSet();
        acceptors.set(0, getN());
        return acceptors;
    }

    public int getLocalId() {
        return ProcessDescriptor.getInstance().localID;
    }

    public boolean isInWindow(int instanceId) {
        return instanceId < firstUncommitted + ProcessDescriptor.getInstance().windowSize;
    }

    public boolean isIdle() {
        return getLog().nextId == firstUncommitted;
    }
}
