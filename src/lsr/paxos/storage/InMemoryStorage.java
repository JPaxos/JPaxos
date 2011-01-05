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
    private long[] epoch = new long[0];

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
        assert lastSnapshot == null || lastSnapshot.compareTo(snapshot) <= 0;
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
            firstUncommitted = Math.max(firstUncommitted, lastSnapshot.getNextInstanceId());

        SortedMap<Integer, ConsensusInstance> logs = log.getInstanceMap();
        while (firstUncommitted < log.getNextId() &&
               logs.get(firstUncommitted).getState() == LogEntryState.DECIDED) {
            firstUncommitted++;
        }
    }

    public BitSet getAcceptors() {
        BitSet acceptors = new BitSet();
        acceptors.set(0, ProcessDescriptor.getInstance().numReplicas);
        return acceptors;
    }

    public long[] getEpoch() {
        return epoch;
    }

    public void setEpoch(long[] epoch) {
        this.epoch = epoch;
    }

    public void updateEpoch(long[] epoch) {
        if (epoch.length != this.epoch.length) {
            throw new IllegalArgumentException("Incorrect epoch length");
        }

        for (int i = 0; i < epoch.length; i++) {
            this.epoch[i] = Math.max(this.epoch[i], epoch[i]);
        }
    }

    public void updateEpoch(long newEpoch, int id) {
        if (id >= epoch.length) {
            throw new IllegalArgumentException("Incorrect id");
        }

        epoch[id] = Math.max(epoch[id], newEpoch);
    }

    public boolean isInWindow(int instanceId) {
        return instanceId < firstUncommitted + ProcessDescriptor.getInstance().windowSize;
    }

    public boolean isIdle() {
        return getLog().nextId == firstUncommitted;
    }
}
