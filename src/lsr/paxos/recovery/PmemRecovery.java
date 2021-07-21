package lsr.paxos.recovery;

import java.io.IOException;

import lsr.common.SingleThreadDispatcher;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.core.Paxos;
import lsr.paxos.replica.DecideCallback;
import lsr.paxos.replica.Replica;
import lsr.paxos.replica.ServiceProxy;
import lsr.paxos.storage.PersistentStorage;
import lsr.paxos.storage.Storage;

public class PmemRecovery extends RecoveryAlgorithm {
    private Paxos paxos;
    private DecideCallback decideCallback;

    public PmemRecovery(SnapshotProvider snapshotProvider, Replica replica,
                        ServiceProxy serviceProxy,
                        SingleThreadDispatcher replicaDispatcher, DecideCallback decideCallback)
            throws IOException {
        this.decideCallback = decideCallback;
        Storage storage = new PersistentStorage();
        paxos = new Paxos(snapshotProvider, storage, replica);
        replicaDispatcher.execute(new Runnable() {
            @Override
            public void run() {
                serviceProxy.doPmemRecovery(paxos.getStorage());
            }
        });
    }

    @Override
    public Paxos getPaxos() {
        return paxos;
    }

    @Override
    public void start() throws IOException {
        decideCallback.scheduleExecuteRequests();
        fireRecoveryFinished();
    }
}
