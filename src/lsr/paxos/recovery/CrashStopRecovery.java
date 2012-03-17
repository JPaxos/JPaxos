package lsr.paxos.recovery;

import java.io.IOException;

import lsr.common.ProcessDescriptor;
import lsr.paxos.Paxos;
import lsr.paxos.Paxos;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.storage.InMemoryStorage;
import lsr.paxos.storage.Storage;

public class CrashStopRecovery extends RecoveryAlgorithm {

    private final Paxos paxos;

    public CrashStopRecovery(SnapshotProvider snapshotProvider) throws IOException {
        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();

        Storage storage = new InMemoryStorage();
        if (storage.getView() % descriptor.numReplicas == descriptor.localId) {
            storage.setView(storage.getView() + 1);
        }

        paxos = new Paxos(snapshotProvider, storage);
    }

    public void start() throws IOException {
        fireRecoveryListener();
    }

    public Paxos getPaxos() {
        return paxos;
    }
}
