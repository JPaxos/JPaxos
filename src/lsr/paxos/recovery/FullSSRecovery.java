package lsr.paxos.recovery;

import java.io.IOException;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.PaxosImpl;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.storage.FullSSDiscWriter;
import lsr.paxos.storage.PublicDiscWriter;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.SynchronousStorage;

public class FullSSRecovery extends RecoveryAlgorithm {
    private final SnapshotProvider snapshotProvider;
    private final DecideCallback decideCallback;
    private final String logPath;
    private PublicDiscWriter publicDiscWriter;

    public FullSSRecovery(SnapshotProvider snapshotProvider, DecideCallback decideCallback,
                          String logPath) {
        this.snapshotProvider = snapshotProvider;
        this.decideCallback = decideCallback;
        this.logPath = logPath;
    }

    public void start() throws IOException {
        Storage storage = createStorage();

        Paxos paxos = new PaxosImpl(decideCallback, snapshotProvider, storage);
        fireRecoveryListener(paxos, publicDiscWriter);
    }

    private Storage createStorage() throws IOException {
        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();

        logger.info("Reading log from: " + logPath);
        FullSSDiscWriter writer = new FullSSDiscWriter(logPath);
        publicDiscWriter = writer;
        Storage storage = new SynchronousStorage(writer);
        if (storage.getView() % descriptor.numReplicas == descriptor.localId)
            storage.setView(storage.getView() + 1);
        return storage;
    }

    private final static Logger logger = Logger.getLogger(FullSSRecovery.class.getCanonicalName());
}
