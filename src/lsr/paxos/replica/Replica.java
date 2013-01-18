package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import lsr.paxos.core.Paxos;
import lsr.paxos.events.AfterCatchupSnapshotEvent;
import lsr.paxos.recovery.CrashStopRecovery;
import lsr.paxos.recovery.EpochSSRecovery;
import lsr.paxos.recovery.FullSSRecovery;
import lsr.paxos.recovery.RecoveryAlgorithm;
import lsr.paxos.recovery.RecoveryListener;
import lsr.paxos.recovery.ViewSSRecovery;
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

    // // // // // // // // // // // // //
    // Internal modules of the replica. //
    // // // // // // // // // // // // //

    private final SnapshotListener2 innerSnapshotListener2;
    private final SnapshotProvider innerSnapshotProvider;

    // // // // // // // // // //
    // Miscellaneous variables //
    // // // // // // // // // //

    /** Location for files that should survive crashes */
    private String stableStoragePath;

    /** Next request to be executed. */
    private int executeUB = 0;

    /** Thread for handling events connected to the replica */
    private final SingleThreadDispatcher replicaDispatcher;

    // // // // // // // // // // // // // // // // // // // //
    // Cached replies and past replies for snapshot creation //
    // // // // // // // // // // // // // // // // // // // //

    /**
     * For each client, keeps the sequence id of the last request executed from
     * the client.
     * 
     * This is accessed by the Selector threads, so it must be thread-safe
     */
    private final Map<Long, Reply> executedRequests =
            new ConcurrentHashMap<Long, Reply>(8192, (float) 0.75, 8);

    /** View on executedDifference row for current instance */
    private ArrayList<Reply> cache;

    /** caches responses for clients, maps instance ID to sent responses */
    private final Map<Integer, List<Reply>> executedDifference =
            new HashMap<Integer, List<Reply>>();

    /**
     * State of the {@link #executedRequests} from the moment when previous
     * snapshot has been created. Used to 'reply' the requests in order to add
     * them as part of new snapshot state
     */
    private final Map<Long, Reply> previousSnapshotExecutedRequests = new HashMap<Long, Reply>();
    private ClientBatchManager batchManager;

    /*
     * Description of the above variables on an example:
     * (previousSnapshot)executedRequests and -> maps clientId to lastReply
     * executedDifference -> map instances to replies
     * 
     * Example state (in one moment):
     * 
     * last snapshot instance - 3 next instance - 6
     * 
     * previousSnapshotExecutedRequests - (1,1#2) (3,3#1)
     * 
     * (client 1 has reply for second request cached, client 3 has reply for
     * first request cached)
     * 
     * executedDifference - ((4: 3#2, 2#1), (5: 3#3, 4#1))
     * 
     * (in instance 4 client 3 received reply for second request and client 2
     * received reply for first request, in instance 5 ...)
     * 
     * executedRequests - (1,1#2) (2,2#1) (3,3#3) (4,4#1)
     * 
     * If a client comes with a request, it's ID is checked against
     * executedRequests.
     * 
     * If the service makes snapshot (eg. after request 2#1), to the snapshot it
     * must be appended that executed request at that time were
     * (previousSnapshotExecutedRequests + part of executedDifference)
     */

    /*
     * TODO: the executedRequests map grows and is NEVER cleared!
     * 
     * For theoretical correctness, it must stay so. In practical approach, give
     * me unbounded storage, limit the overall client count or simply let eat
     * some stale client requests.
     * 
     * Bad solution keeping correctness: record time stamp from client, his
     * request will only be valid for 5 minutes, after that time - go away. If
     * client resends it after 5 minutes, we ignore request. If client changes
     * the time stamp, it's already a new request, right? Client with broken
     * clocks will have bad luck.
     */

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

        stableStoragePath = processDescriptor.logPath + '/' + localId;

        innerSnapshotListener2 = new InnerSnapshotListener2();
        innerSnapshotProvider = new InnerSnapshotProvider();
        replicaDispatcher = new SingleThreadDispatcher("Replica");

        serviceProxy = new ServiceProxy(service, executedDifference, replicaDispatcher);
        serviceProxy.addSnapshotListener(innerSnapshotListener2);

        cache = new ArrayList<Reply>(2048);
        executedDifference.put(executeUB, cache);
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
        logger.info("Recovery phase started.");

        replicaDispatcher.start();

        RecoveryAlgorithm recovery = createRecoveryAlgorithm(processDescriptor.crashModel);

        paxos = recovery.getPaxos();

        decideCallback = new DecideCallbackImpl(paxos, this, executeUB);
        paxos.setDecideCallback(decideCallback);

        batchManager = new ClientBatchManager(paxos, this);
        batchManager.start();
        ClientBatchStore.instance.setClientBatchManager(batchManager);

        recovery.addRecoveryListener(new InnerRecoveryListener());
        recovery.start();
    }

    private RecoveryAlgorithm createRecoveryAlgorithm(CrashModel crashModel) throws IOException {
        switch (crashModel) {
            case CrashStop:
                return new CrashStopRecovery(innerSnapshotProvider);
            case FullSS:
                return new FullSSRecovery(innerSnapshotProvider, stableStoragePath);
            case EpochSS:
                return new EpochSSRecovery(innerSnapshotProvider, stableStoragePath);
            case ViewSS:
                return new ViewSSRecovery(innerSnapshotProvider, stableStoragePath);
            default:
                throw new RuntimeException("Unknown crash model: " + crashModel);
        }
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

    // // // // // // // // // // // //
    // Callback's for JPaxos modules //
    // // // // // // // // // // // //

    /** Called when an instance is NOP, in order to count properly the instances */
    /* package access */void executeNopInstance(final int nextInstance) {
        logger.warning("Executing a nop request. Instance: " + executeUB);
    }

    /* package access */void executeClientBatchAndWait(final int instance,
                                                       final ClientRequest[] requests) {
        replicaDispatcher.executeAndWait(new Runnable() {
            @Override
            public void run() {
                innerExecuteClientBatch(instance, requests);
            }
        });
    }

    /* package access */void instanceExecuted(final int instance) {
        replicaDispatcher.executeAndWait(new Runnable() {
            @Override
            public void run() {
                innerInstanceExecuted(instance);
            }
        });
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

        for (ClientRequest cRequest : requests) {
            RequestId rID = cRequest.getRequestId();
            Reply lastReply = executedRequests.get(rID.getClientId());
            if (lastReply != null) {
                int lastSequenceNumberFromClient = lastReply.getRequestId().getSeqNumber();

                // Do not execute the same request several times.
                if (rID.getSeqNumber() <= lastSequenceNumberFromClient) {
                    logger.warning("Request ordered multiple times. " +
                                   instance + ", " + cRequest +
                                   ", lastSequenceNumberFromClient: " +
                                   lastSequenceNumberFromClient);

                    // Send the cached reply back to the client
                    if (rID.getSeqNumber() == lastSequenceNumberFromClient) {
                        // req manager can be null on fullss disk read
                        if (requestManager != null)
                            requestManager.onRequestExecuted(cRequest, lastReply);
                    }
                    continue;
                }
                // else there is a cached reply, but for a past request only.
            }

            // Executing the request (at last!)
            // Here the replica thread is given to Service.
            byte[] result = serviceProxy.execute(cRequest);

            Reply reply = new Reply(cRequest.getRequestId(), result);

            // add request to executed history
            cache.add(reply);

            executedRequests.put(rID.getClientId(), reply);

            // req manager can be null on fullss disk read
            if (requestManager != null)
                requestManager.onRequestExecuted(cRequest, reply);
        }
    }

    private void innerInstanceExecuted(final int instance) {
        assert executeUB == instance : executeUB + " " + instance;
        // TODO (JK) get rid of unnecessary instance parameter
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Instance finished: " + instance);
        }
        cache = new ArrayList<Reply>(2048);
        executeUB = instance + 1;
        executedDifference.put(executeUB, cache);
        serviceProxy.instanceExecuted(instance);
    }

    /**
     * Listener called after recovery algorithm is finished and paxos can be
     * started.
     */
    private class InnerRecoveryListener implements RecoveryListener {

        public void recoveryFinished() {
            recoverReplica();

            replicaDispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.recoveryFinished();
                }
            });

            ClientRequestBatcher.generateUniqueRunId(paxos.getStorage());

            requestManager = new ClientRequestManager(Replica.this, decideCallback,
                    executedRequests, batchManager);
            paxos.setClientRequestManager(requestManager);

            try {
                NioClientProxy.createIdGenerator(paxos.getStorage());
                clientManager = new NioClientManager(requestManager);
                clientManager.start();
            } catch (IOException e) {
                throw new RuntimeException("Could not prepare the socket for clients! Aborting.");
            }

            logger.info("Recovery phase finished. Starting paxos protocol.");
            paxos.startPaxos();
        }

        private void recoverReplica() {
            Storage storage = paxos.getStorage();

            // we need a read-write copy of the map
            SortedMap<Integer, ConsensusInstance> instances =
                    new TreeMap<Integer, ConsensusInstance>();
            instances.putAll(storage.getLog().getInstanceMap());

            // We take the snapshot
            Snapshot snapshot = storage.getLastSnapshot();
            if (snapshot != null) {
                innerSnapshotProvider.handleSnapshot(snapshot);
                instances = instances.tailMap(snapshot.getNextInstanceId());
            }

            for (ConsensusInstance instance : instances.values()) {
                if (instance.getState() == LogEntryState.DECIDED) {
                    Deque<ClientBatchID> requests = Batcher.unpack(instance.getValue());
                    decideCallback.onRequestOrdered(instance.getId(), requests);
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

            // Get previous snapshot next instance id
            int prevSnapshotNextInstId;
            Snapshot lastSnapshot = paxos.getStorage().getLastSnapshot();
            if (lastSnapshot != null) {
                prevSnapshotNextInstId = lastSnapshot.getNextInstanceId();
            } else {
                prevSnapshotNextInstId = 0;
            }

            // shift previousSnapshotExecutedRequests to moment of snapshot
            for (int i = prevSnapshotNextInstId; i < snapshot.getNextInstanceId(); ++i) {
                List<Reply> ides = executedDifference.remove(i);

                // this is null only when NoOp
                if (ides == null) {
                    continue;
                }

                for (Reply reply : ides) {
                    previousSnapshotExecutedRequests.put(reply.getRequestId().getClientId(), reply);
                }
            }

            snapshot.setLastReplyForClient(previousSnapshotExecutedRequests);

            paxos.onSnapshotMade(snapshot);
        }
    }

    private class InnerSnapshotProvider implements SnapshotProvider {
        public void handleSnapshot(final Snapshot snapshot) {
            logger.info("New snapshot received");
            replicaDispatcher.execute(new Runnable() {
                public void run() {
                    handleSnapshotInternal(snapshot);
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
         */
        private void handleSnapshotInternal(Snapshot snapshot) {
            assert replicaDispatcher.amIInDispatcher();
            assert snapshot != null : "Snapshot is null";

            // TO DO (NS) Obsolete code
            // (JK) ?! wut?
            // FIXME: (JK) check this.

            if (snapshot.getNextInstanceId() < executeUB) {
                logger.warning("Received snapshot is older than current state. " +
                               snapshot.getNextInstanceId() + ", executeUB: " + executeUB);
                return;
            }

            logger.warning("Updating machine state from " + snapshot);
            serviceProxy.updateToSnapshot(snapshot);

            decideCallback.atRestoringStateFromSnapshot(snapshot.getNextInstanceId());

            executedRequests.clear();
            executedDifference.clear();
            executedRequests.putAll(snapshot.getLastReplyForClient());
            previousSnapshotExecutedRequests.clear();
            previousSnapshotExecutedRequests.putAll(snapshot.getLastReplyForClient());
            executeUB = snapshot.getNextInstanceId();

            cache = new ArrayList<Reply>(2048);
            executedDifference.put(executeUB, cache);

            final Object snapshotLock = new Object();

            synchronized (snapshotLock) {
                AfterCatchupSnapshotEvent event = new
                        AfterCatchupSnapshotEvent(snapshot,
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
        }
    }

    private final static Logger logger = Logger.getLogger(Replica.class.getCanonicalName());

    /* package access */boolean hasUnexecutedRequests(ClientRequest[] requests) {
        for (ClientRequest req : requests) {
            RequestId reqId = req.getRequestId();
            Reply prevReply = executedRequests.get(reqId.getClientId());
            if (prevReply == null)
                return true;
            if (prevReply.getRequestId().getSeqNumber() < reqId.getSeqNumber())
                return true;
        }
        return false;
    }

}
