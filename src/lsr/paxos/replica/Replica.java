package lsr.paxos.replica;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.common.ReplicaRequest;
import lsr.common.Reply;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Batcher;
import lsr.paxos.BatcherImpl;
import lsr.paxos.Paxos;
import lsr.paxos.ReplicaCallback;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.events.AfterCatchupSnapshotEvent;
import lsr.paxos.recovery.CrashStopRecovery;
import lsr.paxos.recovery.EpochSSRecovery;
import lsr.paxos.recovery.FullSSRecovery;
import lsr.paxos.recovery.RecoveryAlgorithm;
import lsr.paxos.recovery.RecoveryListener;
import lsr.paxos.recovery.ViewSSRecovery;
import lsr.paxos.statistics.PerformanceLogger;
import lsr.paxos.statistics.ReplicaStats;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.SingleNumberWriter;
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
 * 
 * @author Nuno Santos (LSR)
 */
public class Replica {
    // TODO TZ better comments
    /**
     * Represents different crash models.
     */
    public enum CrashModel {
        /**
         * Synchronous writes to disk are made on critical path of paxos
         * execution. Catastrophic failures will be handled correctly. It is
         * slower than other algorithms.
         */
        FullStableStorage,

        CrashStop,

        EpochSS,

        ViewSS
    }

    private String logPath;

    private Paxos paxos;
    private final ServiceProxy serviceProxy;
    private volatile NioClientManager clientManager;

    private final boolean logDecisions = false;
    /** Used to log all decisions. */
    private final PerformanceLogger decisionsLog;

    /** Next request to be executed. */
    private int executeUB = 0;

    private ClientRequestManager requestManager;

    // TODO: JK check if this map is cleared where possible
    /** caches responses for clients */
    private final NavigableMap<Integer, List<Reply>> executedDifference =
            new TreeMap<Integer, List<Reply>>();

    /**
     * For each client, keeps the sequence id of the last request executed from
     * the client.
     * 
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
     * 
     * This is accessed by the Selector threads, so it must be thread-safe
     */
    private final Map<Long, Reply> executedRequests =
            new ConcurrentHashMap<Long, Reply>();  
    //            new HashMap<Long, Reply>();

    /** Temporary storage for the instances that finished out of order. */
    private final NavigableMap<Integer, Deque<ReplicaRequest>> decidedWaitingExecution =
            new TreeMap<Integer, Deque<ReplicaRequest>>();

    private final HashMap<Long, Reply> previousSnapshotExecutedRequests = new HashMap<Long, Reply>();

    private final SingleThreadDispatcher dispatcher;
    private final ProcessDescriptor descriptor;

    private final ReplicaCallback innerDecideCallback;
    private final SnapshotListener2 innerSnapshotListener2;
    private final SnapshotProvider innerSnapshotProvider;

    private final Configuration config;

    private Vector<Reply> cache;

    /**
     * Initializes new instance of <code>Replica</code> class.
     * <p>
     * This constructor doesn't start the replica and Paxos protocol. In order
     * to run it the {@link #start()} method should be called.
     * 
     * @param config - the configuration of the replica
     * @param localId - the id of replica to create
     * @param service - the state machine to execute request on
     * @throws IOException if an I/O error occurs
     */
    public Replica(Configuration config, int localId, Service service) throws IOException {
        this.innerDecideCallback = new InnerDecideCallback();
        this.innerSnapshotListener2 = new InnerSnapshotListener2();
        this.innerSnapshotProvider = new InnerSnapshotProvider();
        this.dispatcher = new SingleThreadDispatcher("Replica");
        this.config = config;

        ProcessDescriptor.initialize(config, localId);
        descriptor = ProcessDescriptor.getInstance();

        logPath = descriptor.logPath + '/' + localId;

        // Open the log file with the decisions
        if (logDecisions) {
            decisionsLog = PerformanceLogger.getLogger("decisions-" + localId);
        } else {
            decisionsLog = null;
        }

        serviceProxy = new ServiceProxy(service, executedDifference, dispatcher);
        serviceProxy.addSnapshotListener(innerSnapshotListener2);

        cache = new Vector<Reply>();
        executedDifference.put(executeUB, cache);
    }

    /**
     * Starts the replica.
     * <p>
     * First the recovery phase is started and after that the replica joins the
     * Paxos protocol and starts the client manager and the underlying service.
     * 
     * @throws IOException if some I/O error occurs
     */
    public void start() throws IOException {
        logger.info("Recovery phase started.");

        RecoveryAlgorithm recovery = createRecoveryAlgorithm(descriptor.crashModel);
        paxos = recovery.getPaxos();

        // TODO TZ - the dispatcher and network has to be started before
        // recovery phase.
        paxos.getDispatcher().start();
        paxos.getNetwork().start();
        paxos.getCatchup().start();

        recovery.addRecoveryListener(new InnerRecoveryListener());
        recovery.start();
    }

    private RecoveryAlgorithm createRecoveryAlgorithm(CrashModel crashModel) throws IOException {
        switch (crashModel) {
            case CrashStop:
                return new CrashStopRecovery(innerSnapshotProvider, innerDecideCallback);
            case FullStableStorage:
                return new FullSSRecovery(innerSnapshotProvider, innerDecideCallback, logPath);
            case EpochSS:
                return new EpochSSRecovery(innerSnapshotProvider, innerDecideCallback, logPath);
            case ViewSS:
                return new ViewSSRecovery(innerSnapshotProvider, innerDecideCallback,
                        new SingleNumberWriter(logPath, "sync.view"));
            default:
                throw new RuntimeException("Unknown crash model: " + crashModel);
        }
    }

    public void forceExit() {
        dispatcher.shutdownNow();
    }

    /**
     * Sets the path to directory where all logs will be saved.
     * 
     * @param path to directory where logs will be saved
     */
    public void setLogPath(String path) {
        logPath = path;
    }

    /**
     * Gets the path to directory where all logs will be saved.
     * 
     * @return path
     */
    public String getLogPath() {
        return logPath;
    }

    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Processes the requests that were decided but not yet executed.
     */
    void onReplicaRequestDecided() {
        // Called by the protocol thread
        while (true) {
            Deque<ReplicaRequest> batch = decidedWaitingExecution.remove(executeUB);
            if (batch == null) {
                return;
            }

            assert paxos.getStorage().getLog().getNextId() > executeUB;

            if (batch.size() == 1 && batch.getFirst().isNop()) {
                logger.warning("Executing a nop request. Instance: " + executeUB);
                dispatcher.execute(new Runnable() {
                    @Override
                    public void run() {
                        serviceProxy.executeNop();
                    }
                });
                // Give empty array to request manager
                requestManager.onBatchDecided(executeUB, new ArrayDeque<ReplicaRequest>(0));
            } else {
                requestManager.onBatchDecided(executeUB, batch);
            }
            executeUB++;
        }
    }


    public void executeClientBatch(final int instance, final ClientRequest[] batch) {
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                executeClientRequest(instance, batch);                
            }
        });
    }
    
    // Statistics. Used to count how many requests are in a given instance.
    private int requestsInInstance = 0;

    /** Called by RequestManager when it finishes executing a batch */
    public void instanceExecuted(final int instance) {
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Instance finished: " + instance);
                }
                serviceProxy.instanceExecuted(instance);
                cache = new Vector<Reply>();
                executedDifference.put(instance+1, cache);
                
                ReplicaStats.getInstance().setRequestsInInstance(instance, requestsInInstance);
                requestsInInstance=0;                
            }
        });
    }
    
    /** 
     * Called by the RequestManager when it has the ClientRequest that should be
     * executed next. 
     * 
     * @param instance
     * @param batch
     */
    private void executeClientRequest(int instance, ClientRequest[] batch) {
        assert dispatcher.amIInDispatcher() : "Wrong thread: " + Thread.currentThread().getName();

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Executing batch for instance: " + instance);
        }
        
        for (ClientRequest cRequest : batch) {            
            Reply lastReply = executedRequests.get(cRequest.getRequestId().getClientId());
            if (lastReply != null) {
                int lastSequenceNumberFromClient = lastReply.getRequestId().getSeqNumber();
                // prevents executing the same request few times.
                // Do not execute the same request several times.
                if (cRequest.getRequestId().getSeqNumber() <= lastSequenceNumberFromClient) {
                    logger.warning("Request ordered multiple times. Not executing " +
                            instance + ", " + cRequest);
                    continue;
                }
            }

            // Here the replica thread is given to Service.
            byte[] result = serviceProxy.execute(cRequest);
            // Statistics. Count how many requests are in this instance
            requestsInInstance++;

            Reply reply = new Reply(cRequest.getRequestId(), result);

            if (logDecisions) {
                assert decisionsLog != null : "Decision log cannot be null";
                decisionsLog.logln(instance + ":" + cRequest.getRequestId());
            }

            // add request to executed history
            cache.add(reply);

            executedRequests.put(cRequest.getRequestId().getClientId(), reply);

            // Can this ever be null?
            if (requestManager != null) {
                requestManager.onRequestExecuted(cRequest, reply);
            }
        }
    }


    /**
     * Listener called after recovery algorithm is finished and paxos can be
     * started.
     */
    private class InnerRecoveryListener implements RecoveryListener {
        public void recoveryFinished() {
            recoverReplica();

            logger.info("Recovery phase finished. Starting paxos protocol.");
            paxos.startPaxos();

            dispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.recoveryFinished();
                }
            });

            createAndStartClientManager(paxos);
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

            Batcher batcher = new BatcherImpl();
            for (ConsensusInstance instance : instances.values()) {
                if (instance.getState() == LogEntryState.DECIDED) {
                    Deque<ReplicaRequest> requests = batcher.unpack(instance.getValue());
                    innerDecideCallback.onRequestOrdered(instance.getId(), requests);
                }
            }
            storage.updateFirstUncommitted();
        }

        private void createAndStartClientManager(Paxos paxos) {
            IdGenerator idGenerator = createIdGenerator();
            int clientPort = descriptor.getLocalProcess().getClientPort();
            requestManager = new ClientRequestManager(Replica.this, paxos, executedRequests, executeUB);            

            try {
                clientManager = new NioClientManager(clientPort, requestManager, idGenerator);
                clientManager.start();
            } catch (IOException e) {
                throw new RuntimeException("Could not prepare the socket for clients! Aborting.");
            }
        }

        private IdGenerator createIdGenerator() {
            String generatorName = ProcessDescriptor.getInstance().clientIDGenerator;
            if (generatorName.equals("TimeBased")) {
                return new TimeBasedIdGenerator(descriptor.localId, descriptor.numReplicas);
            }
            if (generatorName.equals("Simple")) {
                return new SimpleIdGenerator(descriptor.localId, descriptor.numReplicas);
            }
            throw new RuntimeException("Unknown id generator: " + generatorName +
                    ". Valid options: {TimeBased, Simple}");
        }
    }

    private class InnerDecideCallback implements ReplicaCallback {
        /** Called by the paxos box when a new request is ordered. */
        public void onRequestOrdered(int instance, Deque<ReplicaRequest> values) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Request ordered: " + instance + ":" + values);
            }

            // Execute on the protocol thread.
            // Add the batch to the queue. There may be gaps on the decision sequence.
            decidedWaitingExecution.put(instance, values);
            onReplicaRequestDecided();

            if (instance > paxos.getStorage().getFirstUncommitted()) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Out of order decision. Expected: " +
                            (paxos.getStorage().getFirstUncommitted()));
                }
            }
        }

        @Override
        public void onViewChange(int newView) {
            // The request manager is started only after the initialization/recovery
            // is done. The Paxos protocol starts running before that, so it may happen that
            // this is called while requestManager is still null. There is no problem in
            // ignoring the request because at this point there will not be any client
            // connections, because the client manager is started only after recovery is done
            if (clientManager != null && clientManager.isStarted()) {
                requestManager.onViewChange(newView);
            }
        }
    }

    private class InnerSnapshotListener2 implements SnapshotListener2 {
        public void onSnapshotMade(final Snapshot snapshot) {
            dispatcher.checkInDispatcher();

            if (snapshot.getValue() == null) {
                throw new RuntimeException("Received a null snapshot!");
            }

            // add header to snapshot
            Map<Long, Reply> requestHistory = 
                    new HashMap<Long, Reply>(previousSnapshotExecutedRequests);

            // Get previous snapshot next instance id
            int prevSnapshotNextInstId;
            Snapshot lastSnapshot = paxos.getStorage().getLastSnapshot();
            if (lastSnapshot != null) {
                prevSnapshotNextInstId = lastSnapshot.getNextInstanceId();
            } else {
                prevSnapshotNextInstId = 0;
            }

            // update map to state in moment of snapshot
            for (int i = prevSnapshotNextInstId; i < snapshot.getNextInstanceId(); ++i) {
                List<Reply> ides = executedDifference.remove(i);

                // this is null only when NoOp
                if (ides == null) {
                    continue;
                }

                for (Reply reply : ides) {
                    requestHistory.put(reply.getRequestId().getClientId(), reply);
                }
            }

            snapshot.setLastReplyForClient(requestHistory);

            previousSnapshotExecutedRequests.clear();
            previousSnapshotExecutedRequests.putAll(requestHistory);

            paxos.onSnapshotMade(snapshot);
        }
    }

    private class InnerSnapshotProvider implements SnapshotProvider {
        public void handleSnapshot(final Snapshot snapshot) {
            logger.info("New snapshot received");
            dispatcher.execute(new Runnable() {
                public void run() {
                    handleSnapshotInternal(snapshot);
                }
            });
        }

        public void askForSnapshot() {
            dispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.askForSnapshot();
                }
            });
        }

        public void forceSnapshot() {
            dispatcher.execute(new Runnable() {
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
            assert dispatcher.amIInDispatcher();
            assert snapshot != null : "Snapshot is null";

            if (snapshot.getNextInstanceId() < executeUB) {
                logger.warning("Received snapshot is older than current state." +
                        snapshot.getNextInstanceId() + ", executeUB: " + executeUB);
                return;
            }

            logger.info("Updating machine state from snapshot." + snapshot);
            serviceProxy.updateToSnapshot(snapshot);
            synchronized (decidedWaitingExecution) {
                if (!decidedWaitingExecution.isEmpty()) {
                    if (decidedWaitingExecution.lastKey() < snapshot.getNextInstanceId()) {
                        decidedWaitingExecution.clear();
                    } else {
                        while (decidedWaitingExecution.firstKey() < snapshot.getNextInstanceId()) {
                            decidedWaitingExecution.pollFirstEntry();
                        }
                    }
                }
            }

            executedRequests.clear();
            executedDifference.clear();
            executedRequests.putAll(snapshot.getLastReplyForClient());
            previousSnapshotExecutedRequests.clear();
            previousSnapshotExecutedRequests.putAll(snapshot.getLastReplyForClient());
            executeUB = snapshot.getNextInstanceId();

            final Object snapshotLock = new Object();

            synchronized (snapshotLock) {
                AfterCatchupSnapshotEvent event = new AfterCatchupSnapshotEvent(snapshot,
                        paxos.getStorage(), snapshotLock);
                paxos.getDispatcher().dispatch(event);

                try {
                    while (!event.isFinished()) {
                        snapshotLock.wait();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            onReplicaRequestDecided();
        }
    }

    public SingleThreadDispatcher getReplicaDispatcher() {
        return dispatcher;
    }

    private final static Logger logger = Logger.getLogger(Replica.class.getCanonicalName());


}
