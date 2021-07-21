package lsr.paxos.events;

import static lsr.common.ProcessDescriptor.processDescriptor;

import lsr.common.CrashModel;
import lsr.paxos.Snapshot;
import lsr.paxos.NATIVE.PersistentMemory;
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
        Integer lastSnapshotNextId = storage.getLastSnapshotNextId();
        if (lastSnapshotNextId != null &&
            lastSnapshotNextId >= snapshot.getNextInstanceId()) {
            finished = true;
            synchronized (snapshotLock) {
                snapshotLock.notify();
            }
            return;
        }

        if (processDescriptor.crashModel == CrashModel.Pmem)
            PersistentMemory.startThreadLocalTx();

        storage.setLastSnapshot(snapshot);
        if (lastSnapshotNextId != null) {
            storage.getLog().truncateBelow(lastSnapshotNextId);
        }
        storage.getLog().clearUndecidedBelow(snapshot.getNextInstanceId());
        storage.updateFirstUncommitted();

        if (processDescriptor.crashModel == CrashModel.Pmem)
            PersistentMemory.commitThreadLocalTx();

        finished = true;
        synchronized (snapshotLock) {
            snapshotLock.notify();
        }
    }

    public boolean isFinished() {
        return finished;
    }
}
