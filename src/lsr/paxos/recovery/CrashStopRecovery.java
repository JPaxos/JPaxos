package lsr.paxos.recovery;

import java.io.IOException;

import lsr.common.ProcessDescriptor;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.PaxosImpl;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

public class CrashStopRecovery extends RecoveryAlgorithm {
    private final SnapshotProvider snapshotProvider;
    private final DecideCallback decideCallback;

    public CrashStopRecovery(SnapshotProvider snapshotProvider, DecideCallback decideCallback) {
        this.snapshotProvider = snapshotProvider;
        this.decideCallback = decideCallback;
    }

    public void start() throws IOException {
        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();

        Storage storage = new InMemoryStorage();
        if (storage.getView() % descriptor.numReplicas == descriptor.localId)
            storage.setView(storage.getView() + 1);

        Paxos paxos = new PaxosImpl(decideCallback, snapshotProvider, storage);
        fireRecoveryListener(paxos, null);
    }
}
