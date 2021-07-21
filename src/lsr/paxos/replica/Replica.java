package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.ClientRequest;
import lsr.common.Configuration;
import lsr.common.CrashModel;
import lsr.common.ProcessDescriptor;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Batcher;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.NATIVE.PersistentMemory;
import lsr.paxos.core.Paxos;
import lsr.paxos.events.AfterCatchupSnapshotEvent;
import lsr.paxos.recovery.CrashStopRecovery;
import lsr.paxos.recovery.EpochSSRecovery;
import lsr.paxos.recovery.FullSSRecovery;
import lsr.paxos.recovery.PmemRecovery;
import lsr.paxos.recovery.RecoveryAlgorithm;
import lsr.paxos.recovery.RecoveryListener;
import lsr.paxos.recovery.ViewSSRecovery;
import lsr.paxos.replica.storage.InMemoryReplicaStorage;
import lsr.paxos.replica.storage.ReplicaStorage;
import lsr.paxos.replica.storage.SnapshotlyPersistentReplicaStorage;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Storage;
import lsr.service.Service;

/**
 * Manages replication of a service. Receives requests from the client, orders
 * them using Paxos, executes the ordered requests and sends the reply back to
 * the client.
 * <p>
 * Example of usage:
 * <p>
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws IOException {
 *  int localId = Integer.parseInt(args[0]);
 *  Replica replica = new Replica(localId, new MapService());
 *  replica.start();
 * }
 * </pre>
 * 
 * </blockquote>
 */
public class Replica {

    // // // // // // // // // // // // // // // //
    // External modules accessed by the replica. //
    // // // // // // // // // // // // // // // //

    private Paxos paxos;
    private NioClientManager clientManager;
    private ClientRequestManager requestManager;
    /** Represent the deterministic state machine (service) itself */
    private final ServiceProxy serviceProxy;
    private DecideCallbackImpl decideCallback;
    /** Client set exposed to the world */
    private InternalClient intCli = null;
    private Batcher batcher;

    // // // // // // // // // // // // //
    // Internal modules of the replica. //
    // // // // // // // // // // // // //

    private final SnapshotListener2 innerSnapshotListener2;
    private final SnapshotProvider innerSnapshotProvider;

    // // // // // // // // // //
    // Miscellaneous variables //
    // // // // // // // // // //

    private final ReplicaStorage replicaStorage;

    /** Location for files that should survive crashes */
    private String stableStoragePath;

    /** Thread for handling events connected to the replica */
    private final SingleThreadDispatcher replicaDispatcher;

    private ClientBatchManager batchManager;
    private ClientRequestForwarder requestForwarder;

    // // // // // // //
    // Public methods //
    // // // // // // //

    /**
     * Initializes new instance of <code>Replica</code> class.
     * <p>
     * This constructor doesn't start the replica and Paxos protocol. In order
     * to run it the {@link #start()} method should be called.
     * 
     * @param config - the configuration of the replica
     * @param localId - the id of replica to create
     * @param service - the state machine to execute request on
     */
    public Replica(Configuration config, int localId, Service service) {
        ProcessDescriptor.initialize(config, localId);
        if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
            logger.warn(processDescriptor.logMark_Benchmark2019, "START");

        stableStoragePath = processDescriptor.logPath + '/' + localId;

        switch (processDescriptor.crashModel) {
            case CrashStop:
            case FullSS:
            case ViewSS:
            case EpochSS:
                replicaStorage = new InMemoryReplicaStorage();
                break;
            case Pmem:
                String pmemFile = processDescriptor.nvmDirectory + '/' + "jpaxos." +
                                  String.valueOf(localId);
                try {
                    PersistentMemory.loadLib(pmemFile);
                } catch (UnsatisfiedLinkError e) {
                    throw new RuntimeException(
                            "Shared library (.so/.dll) for pmem missing or invalid", e);
                } catch (AccessDeniedException e) {
                    throw new RuntimeException("Cannot create pmem file " + pmemFile, e);
                }
                replicaStorage = new SnapshotlyPersistentReplicaStorage();
                break;
            default:
                throw new UnsupportedOperationException();
        }

        innerSnapshotListener2 = new InnerSnapshotListener2();
        innerSnapshotProvider = new InnerSnapshotProvider();
        replicaDispatcher = new SingleThreadDispatcher("Replica");

        serviceProxy = new ServiceProxy(service, replicaStorage, replicaDispatcher);
        serviceProxy.addSnapshotListener(innerSnapshotListener2);
        if (processDescriptor.crashModel == CrashModel.Pmem) {
            assert replicaStorage instanceof SnapshotlyPersistentReplicaStorage;
            SnapshotlyPersistentReplicaStorage sprs = (SnapshotlyPersistentReplicaStorage) replicaStorage;
            serviceProxy.addSnapshotListener(sprs);
        }
    }

    /**
     * Starts the replica.
     * 
     * First the recovery phase is started and after that the replica joins the
     * Paxos protocol and starts the client manager and the underlying service.
     * 
     * @throws IOException if some I/O error occurs
     */
    public void start() throws IOException {
        logger.info(processDescriptor.logMark_Benchmark, "Recovery phase started.");

        replicaDispatcher.start();

        decideCallback = new DecideCallbackImpl(this);

        RecoveryAlgorithm recovery = createRecoveryAlgorithm(processDescriptor.crashModel);

        paxos = recovery.getPaxos();
        paxos.setDecideCallback(decideCallback);

        serviceProxy.setSnapshotMaintainer(paxos.getSnapshotMaintainer());

        batcher = paxos.getBatcher();

        if (processDescriptor.indirectConsensus) {
            batchManager = new ClientBatchManager(paxos, this);
            batchManager.start();
            requestForwarder = null;
            ClientBatchStore.instance.setClientBatchManager(batchManager);
        } else {
            batchManager = null;
            requestForwarder = new ClientRequestForwarder(paxos);
            requestForwarder.start();
        }

        if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019)) {
            paxos.reportDPS();
        }

        paxos.startPassivePaxos();

        recovery.addRecoveryListener(new InnerRecoveryListener());

        if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
            logger.warn(processDescriptor.logMark_Benchmark2019, "REC B");

        recovery.start();
    }

    private RecoveryAlgorithm createRecoveryAlgorithm(CrashModel crashModel) throws IOException {
        switch (crashModel) {
            case CrashStop:
                return new CrashStopRecovery(innerSnapshotProvider, this);
            case FullSS:
                return new FullSSRecovery(innerSnapshotProvider, this, stableStoragePath);
            case EpochSS:
                return new EpochSSRecovery(innerSnapshotProvider, this, stableStoragePath);
            case ViewSS:
                return new ViewSSRecovery(innerSnapshotProvider, this, stableStoragePath);
            case Pmem:
                return new PmemRecovery(innerSnapshotProvider, this, serviceProxy,
                        replicaDispatcher, decideCallback);
            default:
                throw new RuntimeException("Unknown crash model: " + crashModel);
        }
    }

    public ReplicaStorage getReplicaStorage() {
        return replicaStorage;
    }

    public void forceExit() {
        // TODO (JK) hm... implement this?
        replicaDispatcher.shutdownNow();
    }

    /**
     * Sets the path to directory where all stable storage logs will be saved.
     * 
     * @param path to directory where the stable storage logs will be saved
     */
    public void setStableStoragePath(String path) {
        stableStoragePath = path;
    }

    /**
     * Gets the path to directory where all stable storage logs will be saved.
     * 
     * @return path
     */
    public String getStableStoragePath() {
        return stableStoragePath;
    }

    /**
     * Adds the request to the set of requests be executed. If called e(A) e(B),
     * the delivery will be either d(A) d(B) or d(B) d(A).
     * 
     * If the replica crashes before the request is delivered, the request may
     * get lost.
     * 
     * @param requestValue - the exact request that will be delivered to the
     *            Service execute method
     * @throws IllegalStateException if the method is called before the recovery
     *             has finished
     */
    public void executeNonFifo(byte[] requestValue) throws IllegalStateException {
        if (intCli == null)
            throw new IllegalStateException(
                    "Request cannot be executed before recovery has finished");
        intCli.executeNonFifo(requestValue);
    }

    /** Returns the current view */
    public int getView() {
        if (paxos == null)
            throw new IllegalStateException("Replica must be started prior to this call");
        return paxos.getStorage().getView();
    }

    /** Returns the ID of current leader */
    public int getLeader() {
        if (paxos == null)
            throw new IllegalStateException("Replica must be started prior to this call");
        return paxos.getLeaderId();
    }

    /**
     * Adds a listener for leader changes. Allowed after the replica has been
     * started.
     */
    public boolean addViewChangeListener(Storage.ViewChangeListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("The listener cannot be null");
        if (paxos == null)
            throw new IllegalStateException("Replica must be started prior to adding a listener");
        return paxos.getStorage().addViewChangeListener(listener);
    }

    /**
     * Removes a listener previously added by
     * {@link #addViewChangeListener(Storage.ViewChangeListener)}
     */
    public boolean removeViewChangeListener(Storage.ViewChangeListener listener) {
        return paxos.getStorage().removeViewChangeListener(listener);
    }

    // // // // // // // // // // // //
    // Callback's for JPaxos modules //
    // // // // // // // // // // // //

    /* package access */void executeClientBatchAndWait(final int instance,
                                                       final ClientRequest[] requests) {
        innerExecuteClientBatch(instance, requests);
    }

    /* package access */void instanceExecuted(final int instance,
                                              final ClientRequest[] requests) {
        innerInstanceExecuted(instance, requests);
    }

    /* package access */SingleThreadDispatcher getReplicaDispatcher() {
        return replicaDispatcher;
    }

    // // // // // // // // // // // //
    // Internal methods and classes. //
    // // // // // // // // // // // //

    /**
     * Called by the RequestManager when it has the ClientRequest that should be
     * executed next.
     * 
     * @param instance
     * @param bInfo
     */
    private void innerExecuteClientBatch(int instance, ClientRequest[] requests) {
        assert replicaDispatcher.amIInDispatcher() : "Wrong thread: " +
                                                     Thread.currentThread().getName();

        if (logger.isTraceEnabled(processDescriptor.logMark_Benchmark2019nope))
            logger.trace(processDescriptor.logMark_Benchmark2019nope, "IX {}", instance);

        for (ClientRequest cRequest : requests) {
            try {
                if (processDescriptor.crashModel == CrashModel.Pmem)
                    PersistentMemory.startThreadLocalTx();
                RequestId rID = cRequest.getRequestId();
                Integer lastSequenceNumberFromClient = replicaStorage.getLastReplySeqNoForClient(
                        rID.getClientId());
                if (lastSequenceNumberFromClient != null) {

                    // Do not execute the same request several times.
                    if (rID.getSeqNumber() <= lastSequenceNumberFromClient) {
                        // with Pmem this message is normal for the first
                        // instance after recovery
                        logger.warn(
                                "Request ordered multiple times. inst: {}, req: {}, lastSequenceNumberFromClient: {}",
                                instance, cRequest, lastSequenceNumberFromClient);

                        // (JK) FIXME: investigate if the client could get the
                        // response multiple times here.

                        // Send the cached reply back to the client
                        if (rID.getSeqNumber() == lastSequenceNumberFromClient) {
                            // req manager can be null on fullss disk read
                            if (requestManager != null)
                                requestManager.onRequestExecuted(cRequest,
                                        replicaStorage.getLastReplyForClient(rID.getClientId()));
                        }
                        continue;
                    }
                    // else there is a cached reply, but for a past request
                    // only.
                }

                // Executing the request (at last!)
                // Here the replica thread is given to Service.
                byte[] result = serviceProxy.execute(cRequest);

                Reply reply = new Reply(cRequest.getRequestId(), result);

                replicaStorage.setLastReplyForClient(instance, rID.getClientId(), reply);

                // req manager can be null on fullss disk read
                if (requestManager != null)
                    requestManager.onRequestExecuted(cRequest, reply);
            } finally {
                if (processDescriptor.crashModel == CrashModel.Pmem)
                    PersistentMemory.commitThreadLocalTx();
            }
        }
    }

    private void innerInstanceExecuted(final int instance, final ClientRequest[] requests) {
        replicaDispatcher.checkInDispatcher();
        assert replicaStorage.getExecuteUB() == instance : replicaStorage.getExecuteUB() + " " +
                                                           instance;
        logger.info("Instance finished: {}", instance);
        paxos.getProposer().instanceExecuted(instance);
        serviceProxy.instanceExecuted(instance);
        batcher.instanceExecuted(instance, requests);
    }

    /**
     * Listener called after recovery algorithm is finished and paxos can be
     * started.
     */
    private class InnerRecoveryListener implements RecoveryListener {

        public void recoveryFinished() {
            if (CrashModel.FullSS.equals(processDescriptor.crashModel))
                paxos.getDispatcher().executeAndWait(new Runnable() {
                    public void run() {
                        recoverReplicaFromStorage();
                    }
                });

            ClientRequestBatcher.generateUniqueRunId(paxos.getStorage());

            if (processDescriptor.indirectConsensus) {
                requestManager = new ClientRequestManager(Replica.this, decideCallback,
                        batchManager, paxos);
                batchManager.setClientRequestManager(requestManager);
                paxos.setClientRequestManager(requestManager);
            } else {
                requestManager = new ClientRequestManager(Replica.this, decideCallback,
                        requestForwarder, paxos);
                paxos.setClientRequestManager(requestManager);
                if (processDescriptor.redirectClientsFromLeader && paxos.isLeader()) {
                    requestManager.setFendOffClients(true);
                }
                requestForwarder.setClientRequestManager(requestManager);
            }

            intCli = new InternalClient(replicaDispatcher, requestManager);

            try {
                NioClientProxy.createIdGenerator(paxos.getStorage());
                clientManager = new NioClientManager(requestManager);
                clientManager.start();
            } catch (IOException e) {
                throw new RuntimeException("Could not prepare the socket for clients! Aborting.",
                        e);
            }

            logger.info(processDescriptor.logMark_Benchmark,
                    "Recovery phase finished. Starting paxos protocol.");
            if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
                logger.warn(processDescriptor.logMark_Benchmark2019, "REC E");
            paxos.startActivePaxos();

            replicaDispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.recoveryFinished();
                }
            });
        }

        /**
         * Replays storage. Needed in FullSS only, other algorithms do not have
         * storage to replay.
         */
        private void recoverReplicaFromStorage() {
            Storage storage = paxos.getStorage();

            // we need a read-write copy of the map
            TreeMap<Integer, ConsensusInstance> instances = new TreeMap<Integer, ConsensusInstance>();
            instances.putAll(storage.getLog().getInstanceMap());

            // We take the snapshot
            final Snapshot snapshot = storage.getLastSnapshot();
            if (snapshot != null) {
                innerSnapshotProvider.handleSnapshot(snapshot, null);
                // see lsr.paxos.storage.PersistentLog for why line below is out
                // instances = instances.tailMap(snapshot.getNextInstanceId());
                while (!instances.isEmpty() && instances.firstKey() < snapshot.getNextInstanceId())
                    instances.pollFirstEntry();
            }

            for (ConsensusInstance instance : instances.values()) {
                if (instance.getState() == LogEntryState.DECIDED) {
                    decideCallback.onRequestOrdered(instance.getId(), instance);
                }
            }
            storage.updateFirstUncommitted();
        }
    }

    private class InnerSnapshotListener2 implements SnapshotListener2 {
        public void onSnapshotMade(final Snapshot snapshot) {
            replicaDispatcher.checkInDispatcher();

            if (snapshot.getValue() == null) {
                throw new RuntimeException("Received a null snapshot!");
            }

            Map<Long, Reply> clone = new HashMap<Long, Reply>(
                    replicaStorage.getLastRepliesUptoInstance(snapshot.getNextInstanceId()));
            snapshot.setLastReplyForClient(clone);

            if (logger.isTraceEnabled()) {
                logger.trace(snapshot.dump());
            }

            assert snapshot.getPartialResponseCache().size() == snapshot.getNextRequestSeqNo() -
                                                                snapshot.getStartingRequestSeqNo();

            paxos.onSnapshotMade(snapshot);
        }
    }

    private class InnerSnapshotProvider implements SnapshotProvider {
        public void handleSnapshot(final Snapshot snapshot, final Runnable onSnapshotHandled) {
            if (logger.isDebugEnabled()) {
                logger.info("New snapshot received: " + snapshot.toString() +
                            " (at " + Thread.currentThread().getName() + ")");
            } else
                logger.info("New snapshot received: " + snapshot.toString());

            replicaDispatcher.execute(new Runnable() {
                public void run() {
                    logger.debug("New snapshot received: " + snapshot.toString() +
                                 " (at " + Thread.currentThread().getName() + ")");
                    paxos.getStorage().acquireSnapshotMutex();
                    handleSnapshotInternal(snapshot, onSnapshotHandled);
                    paxos.getStorage().releaseSnapshotMutex();
                }
            });
        }

        public void askForSnapshot() {
            replicaDispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.askForSnapshot();
                }
            });
        }

        public void forceSnapshot() {
            replicaDispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.forceSnapshot();
                }
            });
        }

        /**
         * Restoring state from a snapshot
         * 
         * @param snapshot
         * @param onSnapshotHandled
         */
        private void handleSnapshotInternal(Snapshot snapshot, final Runnable onSnapshotHandled) {
            assert replicaDispatcher.amIInDispatcher();
            assert snapshot != null : "Snapshot is null";

            if (snapshot.getNextInstanceId() < replicaStorage.getExecuteUB()) {
                logger.error("Received snapshot is older than current state. {}, executeUB: {}",
                        snapshot.getNextInstanceId(), replicaStorage.getExecuteUB());
                return;
            }

            logger.warn("Updating machine state from {}",
                    logger.isTraceEnabled() ? snapshot.dump() : snapshot);

            if (processDescriptor.crashModel == CrashModel.Pmem)
                PersistentMemory.startThreadLocalTx();

            serviceProxy.updateToSnapshot(snapshot);

            decideCallback.atRestoringStateFromSnapshot(snapshot.getNextInstanceId());

            replicaStorage.restoreFromSnapshot(snapshot);

            if (processDescriptor.crashModel == CrashModel.Pmem)
                PersistentMemory.commitThreadLocalTx();

            final Object snapshotLock = new Object();

            synchronized (snapshotLock) {
                AfterCatchupSnapshotEvent event = new AfterCatchupSnapshotEvent(snapshot,
                        paxos.getStorage(), snapshotLock);
                paxos.getDispatcher().submit(event);

                try {
                    while (!event.isFinished()) {
                        snapshotLock.wait();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (onSnapshotHandled != null)
                onSnapshotHandled.run();
        }
    }

    /* package access */boolean hasUnexecutedRequests(ClientRequest[] requests) {
        for (ClientRequest req : requests) {
            RequestId reqId = req.getRequestId();
            Integer prevReply = replicaStorage.getLastReplySeqNoForClient(reqId.getClientId());
            if (prevReply == null)
                return true;
            if (prevReply < reqId.getSeqNumber())
                return true;
        }
        return false;
    }

    public ClientRequestManager getRequestManager() {
        return requestManager;
    }

    private final static Logger logger = LoggerFactory.getLogger(Replica.class);
}
