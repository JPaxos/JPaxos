package lsr.paxos.events;

import lsr.paxos.Snapshot;
import lsr.paxos.storage.Storage;

public class AfterCatchupSnapshotEvent implements Runnable {
    private final Snapshot snapshot;
    private final Storage storage;
    private final Object snapshotLock;

    public AfterCatchupSnapshotEvent(Snapshot snapshot, Storage storage, Object snapshotLock) {
        this.snapshot = snapshot;
        this.snapshotLock = snapshotLock;
        this.storage = storage;
    }

    public void run() {
        Snapshot lastSnapshot = storage.getLastSnapshot();
        if (lastSnapshot != null &&
            lastSnapshot.getNextInstanceId() >= snapshot.getNextInstanceId()) {
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

        synchronized (snapshotLock) {
            snapshotLock.notify();
        }
    }
}
