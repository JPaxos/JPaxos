package lsr.paxos.storage;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.paxos.Snapshot;
import lsr.paxos.core.Proposer;
import lsr.paxos.core.Proposer.ProposerState;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

public class InMemoryStorage implements Storage {
    // Must be volatile because it is read by other threads
    // other than the Protocol thread without locking.
    protected volatile int view;
    private volatile int firstUncommitted = 0;

    protected Log log;
    private Snapshot lastSnapshot;

    // we need to lock in one thread, and release in another, so lock cannot be
    // used...
    private final Semaphore snapshotLock = new Semaphore(1);

    private Proposer.ProposerState proposerState = ProposerState.INACTIVE;

    private ArrayList<ViewChangeListener> viewChangeListeners = new ArrayList<Storage.ViewChangeListener>();

    // must be non-null for proper serialization - NOPping otherwise
    private long[] epoch = new long[0];

    /**
     * Initializes new instance of <code>InMemoryStorage</code> class with empty
     * log.
     */
    public InMemoryStorage() {
        log = new InMemoryLog();
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

    public Integer getLastSnapshotNextId() {
        if (lastSnapshot == null)
            return null;
        return lastSnapshot.getNextInstanceId();
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
        assert view > this.view : "Cannot set smaller or equal view.";
        this.view = view;
        if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
            logger.warn(processDescriptor.logMark_Benchmark2019, "VIEW {}", view);
        fireViewChangeListeners();
    }

    public int getFirstUncommitted() {
        return firstUncommitted;
    }

    public void updateFirstUncommitted() {
        if (lastSnapshot != null) {
            firstUncommitted = Math.max(firstUncommitted, lastSnapshot.getNextInstanceId());
        }

        SortedMap<Integer, ? extends ConsensusInstance> logs = log.getInstanceMap();
        while (firstUncommitted < log.getNextId() &&
               logs.get(firstUncommitted).getState() == LogEntryState.DECIDED) {
            firstUncommitted++;
        }
    }

    public long[] getEpoch() {
        return epoch;
    }

    public void setEpoch(long[] epoch) {
        this.epoch = epoch;
    }

    public void updateEpoch(long[] epoch) {
        assert epoch.length == this.epoch.length : "Incorrect epoch length";

        for (int i = 0; i < epoch.length; i++) {
            this.epoch[i] = Math.max(this.epoch[i], epoch[i]);
        }
    }

    public void updateEpoch(long newEpoch, int id) {
        assert id < epoch.length : "Incorrect id";

        epoch[id] = Math.max(epoch[id], newEpoch);
    }

    public boolean isInWindow(int instanceId) {
        return instanceId < firstUncommitted + processDescriptor.windowSize;
    }

    public int getWindowUsed() {
        return getLog().getNextId() - getFirstUncommitted();
    }

    public boolean isWindowFull() {
        return getWindowUsed() == processDescriptor.windowSize;
    }

    public boolean isIdle() {
        return getLog().getNextId() == firstUncommitted;
    }

    public boolean addViewChangeListener(ViewChangeListener l) {
        if (viewChangeListeners.contains(l))
            return false;
        return viewChangeListeners.add(l);
    }

    public boolean removeViewChangeListener(ViewChangeListener l) {
        return viewChangeListeners.remove(l);
    }

    protected void fireViewChangeListeners() {
        for (ViewChangeListener l : viewChangeListeners)
            l.viewChanged(view, processDescriptor.getLeaderOfView(view));
    }

    public long getRunUniqueId() {
        long base = 0;
        switch (processDescriptor.crashModel) {
            case FullSS:
                base = getEpoch()[0];
                break;
            case ViewSS:
                base = getView();
                break;
            case EpochSS:
                base = getEpoch()[processDescriptor.localId];
                break;
            case CrashStop:
                break;
            default:
                throw new RuntimeException();
        }
        return base;
    }

    @Override
    public ProposerState getProposerState() {
        return proposerState;
    }

    @Override
    public void setProposerState(ProposerState proposerState) {
        this.proposerState = proposerState;
    }

    @Override
    public void acquireSnapshotMutex() {
        try {
            snapshotLock.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "Locks can't be interrupted, and semaphores can? That's inconsistent!");
        }
    }

    @Override
    public void releaseSnapshotMutex() {
        snapshotLock.release();
    }

    private final static Logger logger = LoggerFactory.getLogger(InMemoryStorage.class);
}
