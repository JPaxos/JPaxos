package lsr.paxos.recovery;

import java.io.IOException;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.paxos.Paxos;
import lsr.paxos.PaxosImpl;
import lsr.paxos.ReplicaCallback;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.storage.FullSSDiscWriter;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.SynchronousStorage;

public class FullSSRecovery extends RecoveryAlgorithm {
    private final String logPath;
    private Paxos paxos;

    public FullSSRecovery(SnapshotProvider snapshotProvider, ReplicaCallback decideCallback,
                          String logPath) throws IOException {
        this.logPath = logPath;
        Storage storage = createStorage();
        paxos = new PaxosImpl(decideCallback, snapshotProvider, storage);

    }

    public void start() throws IOException {
        fireRecoveryListener();
    }

    private Storage createStorage() throws IOException {
        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();

        logger.info("Reading log from: " + logPath);
        FullSSDiscWriter writer = new FullSSDiscWriter(logPath);
        Storage storage = new SynchronousStorage(writer);
        if (storage.getView() % descriptor.numReplicas == descriptor.localId) {
            storage.setView(storage.getView() + 1);
        }
        return storage;
    }

    private final static Logger logger = Logger.getLogger(FullSSRecovery.class.getCanonicalName());

    @Override
    public Paxos getPaxos() {
        return paxos;
    }
}
