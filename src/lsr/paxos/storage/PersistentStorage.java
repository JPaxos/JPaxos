package lsr.paxos.storage;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.paxos.Snapshot;
import lsr.paxos.core.Proposer;
import lsr.paxos.core.Proposer.ProposerState;

public class PersistentStorage implements Storage {
    public PersistentStorage() {
        switch (getProposerState_()) {
            case Proposer.ENUM_PROPOSERSTATE_INACTIVE:
                proposerStateCache = ProposerState.INACTIVE;
                break;
            case Proposer.ENUM_PROPOSERSTATE_PREPARING:
                proposerStateCache = ProposerState.PREPARING;
                break;
            case Proposer.ENUM_PROPOSERSTATE_PREPARED:
                proposerStateCache = ProposerState.PREPARED;
                break;
            default:
                throw new RuntimeException(
                        "Unknown proposer state: " + String.valueOf(getProposerState_()));
        }
    }

    PersistentLog log = new PersistentLog();

    @Override
    public PersistentLog getLog() {
        return log;
    }

    private static native int getFirstUncommitted_();

    protected int firstUncommittedCache = getFirstUncommitted_();

    @Override
    public int getFirstUncommitted() {
        return firstUncommittedCache;
    }

    private static native int updateFirstUncommitted_();

    @Override
    public void updateFirstUncommitted() {
        firstUncommittedCache = updateFirstUncommitted_();
    }

    @Override
    public boolean isInWindow(int instanceId) {
        return instanceId < firstUncommittedCache + processDescriptor.windowSize;
    }

    @Override
    public int getWindowUsed() {
        return getLog().getNextId() - getFirstUncommitted();
    }

    @Override
    public boolean isWindowFull() {
        return getWindowUsed() == processDescriptor.windowSize;
    }

    @Override
    public boolean isIdle() {
        return getLog().getNextId() == firstUncommittedCache;
    }

    /// returns nextInstanceId or -1 if no snapshot exists
    private static native int getLastSnapshotNextId_();

    @Override
    public Integer getLastSnapshotNextId() {
        int lsni = getLastSnapshotNextId_();
        return lsni == -1 ? null : lsni;
    }

    /// returns last snapshot or array of zero length if no snapshot exists
    private static native byte[] getLastSnapshot_();

    @Override
    public Snapshot getLastSnapshot() {
        byte[] lastSnapshot = getLastSnapshot_();
        if (lastSnapshot.length == 0)
            return null;
        try {
            return new Snapshot(new DataInputStream(new ByteArrayInputStream(lastSnapshot)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static native void setLastSnapshot(int nextInstanceId, ByteBuffer directBB, int size);

    @Override
    public void setLastSnapshot(Snapshot snapshot) {
        assert (getLastSnapshotNextId() == null ||
                snapshot.getNextInstanceId() >= getLastSnapshotNextId());
        int byteSize = snapshot.byteSize();
        ByteBuffer bb = ByteBuffer.allocateDirect(byteSize);
        snapshot.writeTo(bb);
        setLastSnapshot(snapshot.getNextInstanceId(), bb, byteSize);
    }

    protected List<ViewChangeListener> viewChangeListenets = new ArrayList<ViewChangeListener>();

    @Override
    public boolean addViewChangeListener(ViewChangeListener l) {
        return viewChangeListenets.add(l);
    }

    @Override
    public boolean removeViewChangeListener(ViewChangeListener l) {
        return viewChangeListenets.remove(l);
    }

    private static native int getView_();

    protected int viewCache = getView_();

    @Override
    public int getView() {
        return viewCache;
    }

    private static native void setView_(int view);

    @Override
    public void setView(int view) {
        setView_(view);
        if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
            logger.warn(processDescriptor.logMark_Benchmark2019, "VIEW {}", view);
        viewCache = view;
        for (ViewChangeListener l : viewChangeListenets)
            l.viewChanged(view, processDescriptor.getLeaderOfView(view));
    }

    @Override
    public long[] getEpoch() {
        return new long[0];
    }

    @Override
    public void setEpoch(long[] epoch) {
        throw new UnsupportedOperationException("pmem should not use epoch");
    }

    @Override
    public void updateEpoch(long[] epoch) {
        throw new UnsupportedOperationException("pmem should not use epoch");
    }

    @Override
    public void updateEpoch(long epoch, int sender) {
        throw new UnsupportedOperationException("pmem should not use epoch");
    }

    @Override
    public native long getRunUniqueId();

    private ProposerState proposerStateCache;

    private native byte getProposerState_();

    @Override
    public ProposerState getProposerState() {
        return proposerStateCache;
    }

    private native void setProposerState(byte proposerState);

    @Override
    public void setProposerState(ProposerState proposerState) {
        proposerStateCache = proposerState;
        switch (proposerState) {
            case INACTIVE:
                setProposerState(Proposer.ENUM_PROPOSERSTATE_INACTIVE);
                break;
            case PREPARING:
                setProposerState(Proposer.ENUM_PROPOSERSTATE_PREPARING);
                break;
            case PREPARED:
                setProposerState(Proposer.ENUM_PROPOSERSTATE_PREPARED);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    // we need to lock in one thread, and release in another, so lock cannot be
    // used...
    private final Semaphore snapshotLock = new Semaphore(1);

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

    private final static Logger logger = LoggerFactory.getLogger(PersistentStorage.class);
}
