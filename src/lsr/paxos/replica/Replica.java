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
import java.util.concurrent.ExecutionException;
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
import lsr.paxos.PaxosImpl;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.events.AfterCatchupSnapshotEvent;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.FullSSDiscWriter;
import lsr.paxos.storage.PublicDiscWriter;
import lsr.paxos.storage.SimpleStorage;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.SynchronousStableStorage;
import lsr.paxos.storage.UnstableStorage;
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
 * 	int localId = Integer.parseInt(args[0]);
 * 	Replica replica = new Replica(localId, new MapService());
 * 	replica.run(); // or replica.start();
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Nuno Santos (LSR)
 */
public class Replica implements DecideCallback, SnapshotListener2, SnapshotProvider {
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

		/**
		 * No writes to disk are made on critical path. The majority of process
		 * has to be correct at every moment of execution. It is much faster
		 * than FullStableStorage algorithm.
		 */
		WithoutStableStorage,

		CrashStop
	}

	private CrashModel crashModel = CrashModel.CrashStop;
	private String logPath;

	private Paxos paxos;
	private final ServiceProxy serviceProxy;

	private final boolean logDecisions = false;
	/** Used to log all decisions. */
	private final PrintStream decisionsLog;

	/** Next request to be executed. */
	private int executeUB = 0;

	private ReplicaCommandCallback commandCallback;

	// TODO: JK check if this map is cleared where possible
	/** caches responses for clients */
	private final NavigableMap<Integer, List<Reply>> executedDifference = new TreeMap<Integer, List<Reply>>();

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
	private final ConcurrentHashMap<Long, Reply> executedRequests = new ConcurrentHashMap<Long, Reply>();

	/** Temporary storage for the instances that finished out of order. */
	private final NavigableMap<Integer, Deque<Request>> decidedWaitingExecution = new TreeMap<Integer, Deque<Request>>();

	private final SingleThreadDispatcher dispatcher;
	private final ProcessDescriptor descriptor;

	/** Indicates if the replica is currently recovering */
	private boolean recoveryPhase = false;

	private PublicDiscWriter publicDiscWriter;

	/**
	 * Creates new replica.
	 * <p>
	 * This constructor doesn't start the replica and Paxos protocol. In order
	 * to run it the {@link #start()} method should be called.
	 * 
	 * @param processes
	 *            - informations about other replicas
	 * @param localId
	 *            - the id of replica to create
	 * @param service
	 *            - state machine to execute request on
	 * @throws IOException
	 *             - if an I/O error occurs
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public Replica(Configuration config, int localId, Service service) throws IOException {
		dispatcher = new SingleThreadDispatcher("Replica");
		ProcessDescriptor.initialize(config, localId);
		descriptor = ProcessDescriptor.getInstance();

		// Open the log file with the decisions
		if (logDecisions)
			decisionsLog = new PrintStream(new FileOutputStream("decisions-" + localId + ".log"));
		else
			decisionsLog = null;

		serviceProxy = new ServiceProxy(service, executedDifference, dispatcher);
	}

	/**
	 * Starts the replica.
	 * <p>
	 * First the recovery phase is started and after that the replica joins the
	 * Paxos protocol and starts the client manager and the underlying service.
	 * 
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public void start() throws IOException {
		_logger.info("Recovery phase started.");
		// TODO TZ recovery phase
		// synchronous disc
		// for each instance load (id, view, value)
		// load last view
		// asynchronous disc
		// load snapshot
		// load decided instance

		Storage storage = createStorage();

		// TODO TZ recovery

		paxos = createPaxos(storage);
		serviceProxy.addSnapshotListener(this);
		commandCallback = new ReplicaCommandCallback(paxos, executedRequests);

		if (recoveryPhase) {

			// we need a read-write copy of the map
			SortedMap<Integer, ConsensusInstance> instances = new TreeMap<Integer, ConsensusInstance>();
			instances.putAll(storage.getLog().getInstanceMap());

			// We take the snapshot
			Snapshot snapshot = storage.getStableStorage().getLastSnapshot();

			if (snapshot != null) {
				handleSnapshot(snapshot);
				instances = instances.tailMap(snapshot.nextIntanceId);
			}

			Batcher batcher = new BatcherImpl(ProcessDescriptor.getInstance().batchingLevel);
			for (ConsensusInstance instance : instances.values()) {
				if (instance.getState() == LogEntryState.DECIDED) {
					Deque<Request> requests = batcher.unpack(instance.getValue());

					onRequestOrdered(instance.getId(), requests);
				}
			}
			paxos.getStorage().updateFirstUncommitted();

			onRecoveryFinished();
		}

		_logger.info("Recovery phase finished. Starting paxos protocol.");
		paxos.startPaxos();

		int clientPort = descriptor.getLocalProcess().getClientPort();

		IdGenerator idGenerator;
		String idGen = ProcessDescriptor.getInstance().clientIDGenerator;
		if (idGen.equals("TimeBased")) {
			idGenerator = new TimeBasedIdGenerator(descriptor.localID, descriptor.config.getN());
		} else if (idGen.equals("Simple")) {
			idGenerator = new SimpleIdGenerator(descriptor.localID, descriptor.config.getN());
		} else {
			throw new RuntimeException("Unknown id generator: " + idGen + ". Valid options: {TimeBased, Simple}");
		}

		(new NioClientManager(clientPort, commandCallback, idGenerator)).start();

	}

	private void onRecoveryFinished() {
		dispatcher.execute(new Runnable() {
			@Override
			public void run() {
				// TODO: JK check if this is correct
				recoveryPhase = false;
				serviceProxy.recoveryFinished();
			}
		});

	}

	private Storage createStorage() throws IOException {
		StableStorage stableStorage;
		Storage storage;
		switch (crashModel) {
			case CrashStop:
				stableStorage = new UnstableStorage();
				storage = new SimpleStorage(stableStorage);
				if (stableStorage.getView() % storage.getN() == descriptor.localID)
					stableStorage.setView(stableStorage.getView() + 1);
				return storage;
			case FullStableStorage:
				recoveryPhase = true;
				_logger.info("Reading log from: " + logPath);
				FullSSDiscWriter writer = new FullSSDiscWriter(logPath);
				stableStorage = new SynchronousStableStorage(writer);
				publicDiscWriter = writer;
				storage = new SimpleStorage(stableStorage);
				if (stableStorage.getView() % storage.getN() == descriptor.localID)
					stableStorage.setView(stableStorage.getView() + 1);
				return storage;
		}
		throw new RuntimeException("Specified crash model is not implemented yet");
	}

	protected Paxos createPaxos(Storage storage) throws IOException {
		// TODO: cleaner way of choosing modular vs monolithich paxos
		// return new ModularPaxos(p.localID, processes, this, service, this);
		return new PaxosImpl(this, this, storage);
	}

	/**
	 * Sets the crash model handled by replica and paxos.
	 * 
	 * @param crashModel
	 *            the crash model to set
	 */
	public void setCrashModel(CrashModel crashModel) {
		this.crashModel = crashModel;
	}

	/**
	 * Gets the recovery algorithm which will be used by replica and paxos.
	 * 
	 * @return the recoveryAlgorithm
	 */
	public CrashModel getCrashModel() {
		return crashModel;
	}

	/**
	 * Sets the path to directory where all logs will be saved.
	 * 
	 * @param path
	 *            to directory where logs will be saved
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

	// TODO TZ this logic should be moved somewhere
	/*
	 * =================== Replica logic ====================
	 */

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
				if (lastSequenceNumberFromClient != null
						&& request.getRequestId().getSeqNumber() <= lastSequenceNumberFromClient) {
					_logger.warning("Request ordered multiple times. Not executing " + executeUB + ", " + request);
					continue;
				}

				// Here the replica thread is given to Service.
				byte[] result = serviceProxy.execute(request, executeUB);

				Reply reply = new Reply(request.getRequestId(), result);
				if (!BENCHMARK) {
					if (_logger.isLoggable(Level.FINE)) {
						// _logger.fine("Executed #" + _executeUB + ", id=" +
						// request.getRequestId() + ", req="
						// + request.getValue() + ", reply=" + result);
						_logger.info("Executed id=" + request.getRequestId());
					}
				}

				if (logDecisions) {
					assert decisionsLog != null : "Decision log cannot be null";
					decisionsLog.println(executeUB + ":" + request.getRequestId());
				}

				// add request to executed history
				cache.add(reply);

				executedRequests.put(request.getRequestId().getClientId(), reply);

				commandCallback.handleReply(request, reply);
			}

			if (!BENCHMARK) {
				if (_logger.isLoggable(Level.INFO)) {
					_logger.info("Batch done " + executeUB);
				}
			}
			// batching requests: inform the service that all requests assigned
			serviceProxy.instanceExecuted(executeUB);

			executeUB++;
		}
	}

	/** Called by the paxos box when a new request is ordered. */
	public void onRequestOrdered(int instance, Deque<Request> values) {
		if (_logger.isLoggable(Level.FINE)) {
			_logger.fine("Request ordered: " + instance + ":" + values);
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
			if (_logger.isLoggable(Level.INFO)) {
				_logger.info("Out of order decision. Expected: " + (paxos.getStorage().getFirstUncommitted()));
			}
		}
	}

	/*
	 * =================== Snapshot support ====================
	 */
	// public void onSnapshotMade(final int nextInstanceId, final byte[]
	// snapshotValue) {
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

	public void handleSnapshot(final Snapshot snapshot) {
		_logger.info("New snapshot received");
		dispatcher.execute(new Runnable() {
			@Override
			public void run() {
				handleSnapshotInternal(snapshot);
			}
		});
	}

	public void askForSnapshot(final int lastSnapshotInstance) {
		dispatcher.execute(new Runnable() {
			public void run() {
				_logger.fine("State machine asked for snapshot " + lastSnapshotInstance);
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
			_logger.warning("Received snapshot is older than current state." + snapshot.nextIntanceId + ", executeUB: "
					+ executeUB);
			return;
		}

		_logger.info("Updating machine state from snapshot." + snapshot);
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

		final Object _snapshotLock = new Object();

		synchronized (_snapshotLock) {
			paxos.getDispatcher()
					.dispatch(new AfterCatchupSnapshotEvent(snapshot, paxos.getStorage(), _snapshotLock));

			try {
				_snapshotLock.wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		executeDecided();
	}

	public void forceExit() {
		dispatcher.shutdownNow();
	}

	private final static Logger _logger = Logger.getLogger(Replica.class.getCanonicalName());
}
