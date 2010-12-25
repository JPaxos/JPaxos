package lsr.paxos.replica;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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

import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Batcher;
import lsr.paxos.BatcherImpl;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.events.AfterCatchupSnapshotEvent;
import lsr.paxos.recovery.CrashStopRecovery;
import lsr.paxos.recovery.EpochSSRecovery;
import lsr.paxos.recovery.FullSSRecovery;
import lsr.paxos.recovery.RecoveryAlgorithm;
import lsr.paxos.recovery.RecoveryListener;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.PublicDiscWriter;
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
    final static boolean BENCHMARK = true;

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

        EpochSS
    }

    private String logPath;

    private Paxos paxos;
    private final ServiceProxy serviceProxy;
    private NioClientManager clientManager;

    private final boolean logDecisions = false;
    /** Used to log all decisions. */
    private final PrintStream decisionsLog;

    /** Next request to be executed. */
    private int executeUB = 0;

    private ReplicaCommandCallback commandCallback;

    // TODO: JK check if this map is cleared where possible
    /** caches responses for clients */
    private final NavigableMap<Integer, List<Reply>> executedDifference =
            new TreeMap<Integer, List<Reply>>();

    /**
     * For each client, keeps the sequence id of the last request executed from
     * the client.
     * 
     * TODO: the _executedRequests map grows and is NEVER cleared!
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
    private final ConcurrentHashMap<Long, Reply> executedRequests =
            new ConcurrentHashMap<Long, Reply>();

    /** Temporary storage for the instances that finished out of order. */
    private final NavigableMap<Integer, Deque<Request>> decidedWaitingExecution =
            new TreeMap<Integer, Deque<Request>>();

    private final SingleThreadDispatcher dispatcher;
    private final ProcessDescriptor descriptor;

    private PublicDiscWriter publicDiscWriter;

    private DecideCallback innerDecideCallback;
    private SnapshotListener2 innerSnapshotListener2;
    private SnapshotProvider innerSnapshotProvider;

    /**
     * Initializes new instance of <code>Replica</code> class.
     * <p>
     * This constructor doesn't start the replica and Paxos protocol. In order
     * to run it the {@link #start()} method should be called.
     * 
     * @param processes - informations about other replicas
     * @param localId - the id of replica to create
     * @param service - state machine to execute request on
     * @throws IOException - if an I/O error occurs
     */
    public Replica(Configuration config, int localId, Service service) throws IOException {
        innerDecideCallback = new InnerDecideCallback();
        innerSnapshotListener2 = new InnerSnapshotListener2();
        innerSnapshotProvider = new InnerSnapshotProvider();

        dispatcher = new SingleThreadDispatcher("Replica");
        ProcessDescriptor.initialize(config, localId);
        descriptor = ProcessDescriptor.getInstance();

        logPath = descriptor.logPath + '/' + localId;

        // Open the log file with the decisions
        if (logDecisions)
            decisionsLog = new PrintStream(new FileOutputStream("decisions-" + localId + ".log"));
        else
            decisionsLog = null;

        serviceProxy = new ServiceProxy(service, executedDifference, dispatcher);
        serviceProxy.addSnapshotListener(innerSnapshotListener2);
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
        recovery.addRecoveryListener(new InnerRecoveryListener());
        recovery.start();
    }

    private RecoveryAlgorithm createRecoveryAlgorithm(CrashModel crashModel) {
        switch (crashModel) {
            case CrashStop:
                return new CrashStopRecovery(innerSnapshotProvider, innerDecideCallback);
            case FullStableStorage:
                return new FullSSRecovery(innerSnapshotProvider, innerDecideCallback, logPath);
            case EpochSS:
                return new EpochSSRecovery(innerSnapshotProvider, innerDecideCallback, logPath);
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

    /**
     * Returns the public disk writer class if available. May be called after
     * start() method.
     * 
     * If the public disk writer is not available, returns null.
     */
    public PublicDiscWriter getPublicDiscWriter() {
        return publicDiscWriter;
    }

    /**
     * Processes the requests that were decided but not yet executed.
     */
    private void executeDecided() {
        while (true) {
            Deque<Request> requestByteArray;
            synchronized (decidedWaitingExecution) {
                requestByteArray = decidedWaitingExecution.remove(executeUB);
            }

            if (requestByteArray == null) {
                return;
            }

            assert paxos.getStorage().getLog().getNextId() > executeUB;

            // list of executed request in this executeUB instance
            Vector<Reply> cache = new Vector<Reply>();
            executedDifference.put(executeUB, cache);

            for (Request request : requestByteArray) {
                Integer lastSequenceNumberFromClient = null;
                Reply lastReply = executedRequests.get(request.getRequestId().getClientId());
                if (lastReply != null)
                    lastSequenceNumberFromClient = lastReply.getRequestId().getSeqNumber();
                // prevents executing the same request few times.
                // Do not execute the same request several times.
                if (lastSequenceNumberFromClient != null &&
                    request.getRequestId().getSeqNumber() <= lastSequenceNumberFromClient) {
                    logger.warning("Request ordered multiple times. Not executing " + executeUB +
                                   ", " + request);
                    continue;
                }

                // Here the replica thread is given to Service.
                byte[] result = serviceProxy.execute(request, executeUB);

                Reply reply = new Reply(request.getRequestId(), result);
                if (!BENCHMARK) {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.info("Executed id=" + request.getRequestId());
                    }
                }

                if (logDecisions) {
                    assert decisionsLog != null : "Decision log cannot be null";
                    decisionsLog.println(executeUB + ":" + request.getRequestId());
                }

                // add request to executed history
                cache.add(reply);

                executedRequests.put(request.getRequestId().getClientId(), reply);

                if (commandCallback != null)
                    commandCallback.handleReply(request, reply);
            }

            if (!BENCHMARK) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Batch done " + executeUB);
                }
            }
            // batching requests: inform the service that all requests assigned
            serviceProxy.instanceExecuted(executeUB);

            executeUB++;
        }
    }

    /**
     * Listener called after recovery algorithm is finished and paxos can be
     * started.
     */
    private class InnerRecoveryListener implements RecoveryListener {
        public void recoveryFinished(Paxos paxos, PublicDiscWriter publicDiscWriter) {
            Replica.this.paxos = paxos;
            Replica.this.publicDiscWriter = publicDiscWriter;

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
                instances = instances.tailMap(snapshot.nextIntanceId);
            }

            Batcher batcher = new BatcherImpl(ProcessDescriptor.getInstance().batchingLevel);
            for (ConsensusInstance instance : instances.values()) {
                if (instance.getState() == LogEntryState.DECIDED) {
                    Deque<Request> requests = batcher.unpack(instance.getValue());
                    innerDecideCallback.onRequestOrdered(instance.getId(), requests);
                }
            }
            storage.updateFirstUncommitted();
        }

        private void createAndStartClientManager(Paxos paxos) {
            IdGenerator idGenerator = createIdGenerator();
            int clientPort = descriptor.getLocalProcess().getClientPort();
            commandCallback = new ReplicaCommandCallback(paxos, executedRequests);

            try {
                clientManager = new NioClientManager(clientPort, commandCallback, idGenerator);
                clientManager.start();
            } catch (IOException e) {
                logger.severe("Could not prepare the socket for clients! Aborting.");
                System.exit(-1);
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

    private class InnerDecideCallback implements DecideCallback {
        /** Called by the paxos box when a new request is ordered. */
        public void onRequestOrdered(int instance, Deque<Request> values) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Request ordered: " + instance + ":" + values);
            }

            // Simply notify the replica thread that a new request was ordered.
            // The replica thread will then try to execute
            synchronized (decidedWaitingExecution) {
                decidedWaitingExecution.put(instance, values);
            }

            dispatcher.execute(new Runnable() {
                public void run() {
                    executeDecided();
                }
            });

            if (instance > paxos.getStorage().getFirstUncommitted()) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Out of order decision. Expected: " +
                                 (paxos.getStorage().getFirstUncommitted()));
                }
            }
        }
    }

    private class InnerSnapshotListener2 implements SnapshotListener2 {
        public void onSnapshotMade(final Snapshot snapshot) {
            dispatcher.checkInDispatcher();

            if (snapshot.value == null)
                throw new RuntimeException("Received a null snapshot!");

            // Structure for holding all snapshot-related data.
            // Snapshot snapshot = new Snapshot();

            // add header to snapshot
            Map<Long, Reply> requestHistory = new HashMap<Long, Reply>(executedRequests);

            // update map to state in moment of snapshot

            for (int i = executeUB - 1; i >= snapshot.nextIntanceId; i--) {
                List<Reply> ides = executedDifference.get(i);

                // this is null only when NoOp
                if (ides == null)
                    continue;

                for (Reply reply : ides) {
                    requestHistory.put(reply.getRequestId().getClientId(), reply);
                }
            }

            while (executedDifference.firstKey() < snapshot.nextIntanceId) {
                executedDifference.pollFirstEntry();
            }

            snapshot.lastReplyForClient = requestHistory;

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

        public void askForSnapshot(final int lastSnapshotInstance) {
            dispatcher.execute(new Runnable() {
                public void run() {
                    logger.fine("State machine asked for snapshot " + lastSnapshotInstance);
                    serviceProxy.askForSnapshot(lastSnapshotInstance);
                }
            });
        }

        public void forceSnapshot(final int lastSnapshotInstance) {
            dispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.forceSnapshot(lastSnapshotInstance);
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

            if (snapshot.nextIntanceId < executeUB) {
                logger.warning("Received snapshot is older than current state." +
                                snapshot.nextIntanceId + ", executeUB: " + executeUB);
                return;
            }

            logger.info("Updating machine state from snapshot." + snapshot);
            serviceProxy.updateToSnapshot(snapshot);
            synchronized (decidedWaitingExecution) {
                if (!decidedWaitingExecution.isEmpty()) {
                    if (decidedWaitingExecution.lastKey() < snapshot.nextIntanceId)
                        decidedWaitingExecution.clear();
                    else {
                        while (decidedWaitingExecution.firstKey() < snapshot.nextIntanceId) {
                            decidedWaitingExecution.pollFirstEntry();
                        }
                    }
                }

            }

            executedRequests.clear();
            executedDifference.clear();
            executedRequests.putAll(snapshot.lastReplyForClient);
            executeUB = snapshot.nextIntanceId;

            final Object snapshotLock = new Object();

            synchronized (snapshotLock) {
                paxos.getDispatcher().dispatch(
                        new AfterCatchupSnapshotEvent(snapshot, paxos.getStorage(), snapshotLock));

                try {
                    snapshotLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            executeDecided();
        }
    }

    private final static Logger logger = Logger.getLogger(Replica.class.getCanonicalName());

}
