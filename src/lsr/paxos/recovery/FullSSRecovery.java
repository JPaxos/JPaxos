package lsr.paxos.recovery;

import java.io.IOException;
import java.util.Deque;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.paxos.Batcher;
import lsr.paxos.BatcherImpl;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.PaxosImpl;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
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
        // we need a read-write copy of the map
        SortedMap<Integer, ConsensusInstance> instances = new TreeMap<Integer, ConsensusInstance>();
        instances.putAll(storage.getLog().getInstanceMap());

        // We take the snapshot
        Snapshot snapshot = storage.getLastSnapshot();
        if (snapshot != null) {
            snapshotProvider.handleSnapshot(snapshot);
            instances = instances.tailMap(snapshot.nextIntanceId);
        }

        Batcher batcher = new BatcherImpl(ProcessDescriptor.getInstance().batchingLevel);
        for (ConsensusInstance instance : instances.values()) {
            if (instance.getState() == LogEntryState.DECIDED) {
                Deque<Request> requests = batcher.unpack(instance.getValue());
                decideCallback.onRequestOrdered(instance.getId(), requests);
            }
        }
        storage.updateFirstUncommitted();

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
