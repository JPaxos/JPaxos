package lsr.paxos.storage;

import java.util.BitSet;
import java.util.SortedMap;

import lsr.common.ProcessDescriptor;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class InMemoryStorage implements Storage {
    // Must be volatile because it is read by other threads 
    // other than the Protocol thread without locking. 
    protected volatile int view;
    private volatile int firstUncommitted = 0;
    protected Log log;
    private long[] epoch = new long[0];
    
    private final BitSet allProcesses = new BitSet(); 

    /**
     * Initializes new instance of <code>InMemoryStorage</code> class with empty
     * log.
     */
    public InMemoryStorage() {
        log = new Log();
        allProcesses.set(0, ProcessDescriptor.getInstance().numReplicas);
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

    public int getView() {
        return view;
    }

    public void setView(int view) throws IllegalArgumentException {
        if (view <= this.view) {
            throw new IllegalArgumentException("Cannot set smaller or equal view.");
        }
        this.view = view;
    }

    public int getFirstUncommitted() {
        return firstUncommitted;
    }

    public void updateFirstUncommitted() {

        SortedMap<Integer, ConsensusInstance> logs = log.getInstanceMap();
        while (firstUncommitted < log.getNextId() &&
               logs.get(firstUncommitted).getState() == LogEntryState.DECIDED) {
            firstUncommitted++;
        }
    }

    public BitSet getAcceptors() {        
        return (BitSet) allProcesses.clone();
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
    
    public int getWindowUsed() {
        return getLog().getNextId() - getFirstUncommitted();
    }
    
    public boolean isWindowFull() {    
        return getWindowUsed() == ProcessDescriptor.getInstance().windowSize;
    }

    public boolean isIdle() {
        return getLog().nextId == firstUncommitted;
    }
}
