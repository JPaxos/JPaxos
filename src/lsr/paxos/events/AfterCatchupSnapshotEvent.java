package lsr.paxos.events;

import lsr.paxos.Snapshot;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

public class AfterCatchupSnapshotEvent implements Runnable {
    private final Snapshot snapshot;
    private final StableStorage stableStorage;
    private final Storage storage;
    private final Object snapshotLock;

    public AfterCatchupSnapshotEvent(Snapshot snapshot, Storage storage, Object snapshotLock) {
        this.snapshot = snapshot;
        this.snapshotLock = snapshotLock;
        this.storage = storage;
        stableStorage = storage.getStableStorage();
    }

    public void run() {
        Snapshot lastSnapshot = stableStorage.getLastSnapshot();
        if (lastSnapshot != null && lastSnapshot.nextIntanceId >= snapshot.nextIntanceId) {
            synchronized (snapshotLock) {
                snapshotLock.notify();
            }
            return;
        }

        stableStorage.setLastSnapshot(snapshot);
        if (lastSnapshot != null) {
            stableStorage.getLog().truncateBelow(lastSnapshot.nextIntanceId);
        }
        stableStorage.getLog().clearUndecidedBelow(snapshot.nextIntanceId);
        storage.updateFirstUncommitted();

        synchronized (snapshotLock) {
            snapshotLock.notify();
        }
    }
}
