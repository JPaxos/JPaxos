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
        if (lastSnapshot != null && lastSnapshot.nextIntanceId >= snapshot.nextIntanceId) {
            synchronized (snapshotLock) {
                snapshotLock.notify();
            }
            return;
        }

        storage.setLastSnapshot(snapshot);
        if (lastSnapshot != null) {
            storage.getLog().truncateBelow(lastSnapshot.nextIntanceId);
        }
        storage.getLog().clearUndecidedBelow(snapshot.nextIntanceId);
        storage.updateFirstUncommitted();

        synchronized (snapshotLock) {
            snapshotLock.notify();
        }
    }
}
