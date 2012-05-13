package lsr.paxos.replica;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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

import lsr.common.ClientBatch;
import lsr.common.ClientRequest;
import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.Batcher;
import lsr.paxos.Paxos;
import lsr.paxos.recovery.CrashStopRecovery;
import lsr.paxos.recovery.EpochSSRecovery;
import lsr.paxos.recovery.FullSSRecovery;
import lsr.paxos.recovery.RecoveryAlgorithm;
import lsr.paxos.recovery.RecoveryListener;
import lsr.paxos.recovery.ViewSSRecovery;
import lsr.paxos.replica.ClientBatchStore.ClientBatchInfo;
import lsr.paxos.replica.Snapshot;
import lsr.paxos.replica.SnapshotHandle;
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
 */
public class Replica {
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
	
	private int paxosID = 0;
	private int paxosID2 = 0;

	private int nbInstanceExecuted = 0;
	private static final int MAX_INSTANCES = 100;
	
    private String logPath;
	private Snapshot snapshot;

    private Paxos paxos;
    private final ServiceProxy serviceProxy;
    private volatile NioClientManager clientManager;

    private static final boolean LOG_DECISIONS = false;
    /** Used to log all decisions. */
    private final PerformanceLogger decisionsLog;

    /** Next request to be executed. */
    private int executeUB = 0;

    private ClientRequestManager requestManager;

    // TODO: JK check if this map is cleared where possible
    /** caches responses for clients */
    private final Map<Integer, List<Reply>> executedDifference =
            new HashMap<Integer, List<Reply>>();
	
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
            new ConcurrentHashMap<Long, Reply>(8192, (float) 0.75, 8);  



    private final HashMap<Long, Reply> previousSnapshotExecutedRequests = new HashMap<Long, Reply>();

    private final SingleThreadDispatcher dispatcher;
    private final ProcessDescriptor descriptor;
	
    private final Configuration config;

    private ArrayList<Reply> cache;
	
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
        this.dispatcher = new SingleThreadDispatcher("Replica");
        this.config = config;

        ProcessDescriptor.initialize(config, localId);
        descriptor = ProcessDescriptor.getInstance();

        logPath = descriptor.logPath + '/' + localId;

        // Open the log file with the decisions
        if (LOG_DECISIONS) {
            decisionsLog = PerformanceLogger.getLogger("decisions-" + localId);
        } else {
            decisionsLog = null;
        }

        serviceProxy = new ServiceProxy(service, executedDifference, dispatcher);

        cache = new ArrayList<Reply>(2048);
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

        dispatcher.start();
        RecoveryAlgorithm recovery = createRecoveryAlgorithm(descriptor.crashModel);
        paxos = recovery.getPaxos();
		paxos.getCatchup().setReplica(this);
				
        // TODO TZ - the dispatcher and network has to be started before
        // recovery phase.
        // FIXME: NS - For CrashStop this is not needed. For the other recovery algorithms, 
        // this must be fixed. Starting the network before the Paxos module can cause problems, 
        // if some protocol message is received before Paxos has started. These messages will
        // be ignored, which will 
        //        paxos.getDispatcher().start();
        //        paxos.getNetwork().start();
        //        paxos.getCatchup().start();

        recovery.addRecoveryListener(new InnerRecoveryListener());
        recovery.start();
    }

    private RecoveryAlgorithm createRecoveryAlgorithm(CrashModel crashModel) throws IOException {
        switch (crashModel) {
            case CrashStop:
                return new CrashStopRecovery();
            case FullStableStorage:
                return new FullSSRecovery(logPath);
            case EpochSS:
                return new EpochSSRecovery(logPath);
            case ViewSS:
                return new ViewSSRecovery(new SingleNumberWriter(logPath, "sync.view"));
            default:
                throw new RuntimeException("Unknown crash model: " + crashModel);
        }
    }

    public void forceExit() {
        dispatcher.shutdownNow();
    }
	
	public SingleThreadDispatcher getDispatcher() {
        return dispatcher;
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


    public void executeNopInstance(final int nextInstance) {
        logger.warning("Executing a nop request. Instance: " + executeUB);
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                serviceProxy.executeNop();
            }
        });
    }
    
    public void executeClientBatch(final int instance, final ClientBatchInfo bInfo) {
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                innerExecuteClientBatch(instance, bInfo);                
            }
        });
    }

    /** 
     * Called by the RequestManager when it has the ClientRequest that should be
     * executed next. 
     * 
     * @param instance
     * @param bInfo
     */
    private void innerExecuteClientBatch(int instance, ClientBatchInfo bInfo) {
        assert dispatcher.amIInDispatcher() : "Wrong thread: " + Thread.currentThread().getName();

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Executing batch " + bInfo + ", instance number " + instance) ;
        }        
//        StringBuilder sb = new StringBuilder("Executing requests: ");
        for (ClientRequest cRequest : bInfo.batch) {
            RequestId rID = cRequest.getRequestId();
            Reply lastReply = executedRequests.get(rID.getClientId());
            if (lastReply != null) {
                int lastSequenceNumberFromClient = lastReply.getRequestId().getSeqNumber();

                // Do not execute the same request several times.
                if (rID.getSeqNumber() <= lastSequenceNumberFromClient) {
                    logger.warning("Request ordered multiple times. " +
                            instance + ", batch: " + bInfo.bid + ", " + cRequest + ", lastSequenceNumberFromClient: " + lastSequenceNumberFromClient);

                    // Send the cached reply back to the client
                    if (rID.getSeqNumber() == lastSequenceNumberFromClient) {
                        requestManager.onRequestExecuted(cRequest, lastReply);
                    }
                    continue;
                }
            }

//            if (logger.isLoggable(Level.FINE)) {
//                sb.append(cRequest.getRequestId()).append(" ");
//            }
            // Here the replica thread is given to Service.
            byte[] result = serviceProxy.execute(cRequest);
            // Statistics. Count how many requests are in this instance
            requestsInInstance++;

            Reply reply = new Reply(cRequest.getRequestId(), result);

            if (LOG_DECISIONS) {
                assert decisionsLog != null : "Decision log cannot be null";
                decisionsLog.logln(instance + ":" + cRequest.getRequestId());
            }

            // add request to executed history
            cache.add(reply);

            executedRequests.put(rID.getClientId(), reply);

            // Can this ever be null?
            assert requestManager != null : "Request manager should not be null";
            requestManager.onRequestExecuted(cRequest, reply);
        }
//        if (logger.isLoggable(Level.FINE)) {
//            logger.fine(sb.toString());
//        }
    }

    // Statistics. Used to count how many requests are in a given instance.
    private int requestsInInstance = 0;

    /** Called by RequestManager when it finishes executing a batch */
    public void instanceExecuted(final int instance) {        
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                innerInstanceExecuted(instance);
				nbInstanceExecuted++;
				if(nbInstanceExecuted >= MAX_INSTANCES) {
					nbInstanceExecuted = 0;
					doSnapshot(instance);
				}
            }
        });
    }

    void innerInstanceExecuted(final int instance) {
        if (logger.isLoggable(Level.INFO)) {
			System.out.println("Instance finished: " + instance);
            logger.info("Instance finished: " + instance);
        }
        serviceProxy.instanceExecuted(instance);
        cache = new ArrayList<Reply>(2048);
        executedDifference.put(instance+1, cache);
        
        executeUB=instance+1;

        // The ReplicaStats must be updated only from the Protocol thread
        final int fReqCount = requestsInInstance;
        paxos.getDispatcher().submit(new Runnable() {
            @Override
            public void run() {
                ReplicaStats.getInstance().setRequestsInInstance(instance, fReqCount);
            }}  );
        requestsInInstance=0;
    }
    
    /**
     * Listener called after recovery algorithm is finished and paxos can be
     * started.
     */
    private class InnerRecoveryListener implements RecoveryListener {
        public void recoveryFinished() {
            recoverReplica();

            dispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.recoveryFinished();
                }
            });

            IdGenerator idGenerator = createIdGenerator();
            int clientPort = descriptor.getLocalProcess().getClientPort();
            requestManager = new ClientRequestManager(Replica.this, paxos, executedRequests);
            paxos.setClientRequestManager(requestManager);
            paxos.setDecideCallback(requestManager.getClientBatchManager());

            try {
                clientManager = new NioClientManager(clientPort, requestManager, idGenerator);
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
            /*Snapshot snapshot = storage.getLastSnapshot();
            if (snapshot != null) {
                innerSnapshotProvider.handleSnapshot(snapshot);
                instances = instances.tailMap(snapshot.getNextInstanceId());
            }*/

            for (ConsensusInstance instance : instances.values()) {
                if (instance.getState() == LogEntryState.DECIDED) {
                    Deque<ClientBatch> requests = Batcher.unpack(instance.getValue());
                    requestManager.getClientBatchManager().onRequestOrdered(instance.getId(), requests);
                }
            }
            storage.updateFirstUncommitted();
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
				
	public void doSnapshot(int paxosId){
		this.paxosID = paxosId;
		logger.info("Making new snapshot for Paxos instance " + paxosId);
		SnapshotHandle snapshotHandle = new SnapshotHandle(paxosId);
		Snapshot snp = new Snapshot(snapshotHandle, null);
		
		// Create the last requests per client 
		Map<Long,Reply> lastReplyForClient = new HashMap<Long,Reply>();
		
		int j;
		if(snapshot == null) 
			j = 1;
		else {
			SnapshotHandle h = snapshot.getHandle();
			j = h.getPaxosInstanceId();
		}
		
		for (int i = j; i < paxosId; ++i) {
			List<Reply> ides = executedDifference.remove(i);
			if (ides == null) {
				continue;
			}
			for (Reply reply : ides) {
				lastReplyForClient.put(reply.getRequestId().getClientId(), reply);
			}
		}
		snp.setReplyForClient(lastReplyForClient);
		
		// Get data
		byte[] data = serviceProxy.takeSnapshot();
		if(data == null) {
			logger.info("Getting data for snapshot failed. Abort snapshot.");
			return;
		}
		snp.setData(data);
		
		// Save snapshot
		saveSnapshot(snp);

		// Truncate the logs
		paxos.getDispatcher().submit(new Runnable() {
			@Override
			public void run() {
				paxos.getStorage().getLog().truncateBelow(paxosID);
			}
		}  );
		requestManager.getClientBatchManager().getDispatcher().submit(new Runnable() {
			@Override
			public void run() {
				requestManager.getClientBatchManager().truncateBelow(paxosID);
			}
		}  );
		
		logger.info("Snapshot finished for instance " + paxosId);
	}	
	
	public void saveSnapshot(Snapshot snp){
		try {
			FileOutputStream fout = new FileOutputStream("snapshot");
			ObjectOutputStream oos = new ObjectOutputStream(fout);
			oos.writeObject(snp);
			oos.close();
		} catch (Exception e) { 
			e.printStackTrace(); 
		}
		this.snapshot = snp;
	}
	
	public void installSnapshot(int paxosId, byte[] data){
		SnapshotHandle snapshotHandle = new SnapshotHandle(paxosId);
		Snapshot snp = new Snapshot(snapshotHandle, null);
		snp.setData(data);
		saveSnapshot(snp);
		serviceProxy.installSnapshot(paxosId, data);
		
		this.paxosID2 = paxosId;
		requestManager.getClientBatchManager().getDispatcher().submit(new Runnable() {
			@Override
			public void run() {
				requestManager.getClientBatchManager().truncateBelow(paxosID2);
			}
		}  );
	}
	
	public Snapshot getSnapshot(){
		return snapshot;
	}
	
    public SingleThreadDispatcher getReplicaDispatcher() {
        return dispatcher;
    }

    private final static Logger logger = Logger.getLogger(Replica.class.getCanonicalName());

}
