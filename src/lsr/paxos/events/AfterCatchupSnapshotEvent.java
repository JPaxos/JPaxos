package lsr.paxos.events;

import lsr.paxos.Snapshot;
import lsr.paxos.storage.Storage;

public class AfterCatchupSnapshotEvent implements Runnable {
    private final Snapshot snapshot;
    private final Storage storage;
    private final Object snapshotLock;
    private boolean finished;

    public AfterCatchupSnapshotEvent(Snapshot snapshot, Storage storage, Object snapshotLock) {
        this.snapshot = snapshot;
        this.snapshotLock = snapshotLock;
        this.storage = storage;
        this.finished = false;
    }

    public void run() {
        Snapshot lastSnapshot = storage.getLastSnapshot();
        if (lastSnapshot != null &&
            lastSnapshot.getNextInstanceId() >= snapshot.getNextInstanceId()) {
            finished = true;
            synchronized (snapshotLock) {
                snapshotLock.notify();
            }
            return;
        }

        storage.setLastSnapshot(snapshot);
        if (lastSnapshot != null) {
            storage.getLog().truncateBelow(lastSnapshot.getNextInstanceId());
        }
        storage.getLog().clearUndecidedBelow(snapshot.getNextInstanceId());
        storage.updateFirstUncommitted();

        finished = true;
        synchronized (snapshotLock) {
            snapshotLock.notify();
        }
    }

    public boolean isFinished() {
        return finished;
    }
}
