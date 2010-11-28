package lsr.paxos.storage;

import lsr.paxos.Snapshot;

public class UnstableStorage implements StableStorage {
    protected int view;
    protected Log log;
    private Snapshot lastSnapshot;

    public UnstableStorage() {
        log = new Log();
    }

    public UnstableStorage(Log log) {
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
}
