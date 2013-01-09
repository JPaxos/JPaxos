package lsr.paxos.recovery;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.util.logging.Logger;

import lsr.paxos.SnapshotProvider;
import lsr.paxos.core.Paxos;
import lsr.paxos.idgen.IdGeneratorType;
import lsr.paxos.storage.FullSSDiscWriter;
import lsr.paxos.storage.SingleNumberWriter;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.SynchronousStorage;

public class FullSSRecovery extends RecoveryAlgorithm {
    private final String logPath;
    private Paxos paxos;

    public FullSSRecovery(SnapshotProvider snapshotProvider, String logPath)
            throws IOException
    {
        this.logPath = logPath;
        Storage storage = createStorage();
        paxos = new Paxos(snapshotProvider, storage);

    }

    public void start() throws IOException {
        fireRecoveryFinished();
    }

    private Storage createStorage() throws IOException {

        logger.info("Reading log from: " + logPath);
        FullSSDiscWriter writer = new FullSSDiscWriter(logPath);

        Storage storage = new SynchronousStorage(writer);

        if (IdGeneratorType.ViewEpoch.toString().equals(processDescriptor.clientIDGenerator)) {
            SingleNumberWriter epochFile = new SingleNumberWriter(logPath, "sync.epoch");
            storage.setEpoch(new long[] {epochFile.readNumber()});
            epochFile.writeNumber(storage.getEpoch()[0]);
        }

        if (processDescriptor.isLocalProcessLeader(storage.getView())) {
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
