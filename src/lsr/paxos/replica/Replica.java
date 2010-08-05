package lsr.paxos.replica;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Configuration;
import lsr.common.Pair;
import lsr.common.ProcessDescriptor;
import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.PaxosImpl;
import lsr.paxos.SnapshotListener;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.events.AfterCatchupSnapshotEvent;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.DiscWriter;
import lsr.paxos.storage.FullSSDiscWriter;
import lsr.paxos.storage.SimpleStorage;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.SynchronousStableStorage;
import lsr.paxos.storage.UnstableStorage;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
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
public class Replica implements DecideCallback, SnapshotListener,
		SnapshotProvider {
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

	private CrashModel _crashModel = CrashModel.CrashStop;
	private String _logPath;

	private Paxos _paxos;
	private final Service _service;

	private final boolean _logDecisions = false;
	/** Used to log all decisions. */
	private final PrintStream _decisionsLog;

	/** Next request to be executed. */
	private int _executeUB = 0;

	private ReplicaCommandCallback _commandCallback;

	private final SortedMap<Integer, List<RequestId>> _executedDifference = new TreeMap<Integer, List<RequestId>>();

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
	private final Map<Long, Integer> _executedRequests = new HashMap<Long, Integer>();

	/** Temporary storage for the instances that finished out of order. */
	private final NavigableMap<Integer, List<Request>> _decidedWaitingExecution = new TreeMap<Integer, List<Request>>();

	private final SingleThreadDispatcher _dispatcher;
	private final ProcessDescriptor _descriptor;

	/** Indicates if the replica is currently recovering */
	private boolean _recoveryPhase = false;

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
	public Replica(Configuration config, int localId, Service service)
			throws IOException {
		_dispatcher = new SingleThreadDispatcher("Replica");
		_descriptor = new ProcessDescriptor(config, localId);

		// Open the log file with the decisions
		if (_logDecisions)
			_decisionsLog = new PrintStream(new FileOutputStream("decisions-"
					+ localId + ".log"));
		else
			_decisionsLog = null;

		_service = service;
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
	public Storage start() throws IOException {
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

		_paxos = createPaxos(storage);
		_service.addSnapshotListener(this);
		_commandCallback = new ReplicaCommandCallback(_paxos);

		SortedMap<Integer, ConsensusInstance> instances = storage.getLog()
				.getInstanceMap();
		Set<Integer> readInstanceIds = new TreeSet<Integer>();
		readInstanceIds.addAll(instances.keySet());

		if (_recoveryPhase) {
			Pair<Integer, byte[]> snapshot = storage.getStableStorage()
					.getLastSnapshot();

			int snapshotInstanceId = -1;

			if (snapshot != null) {
				snapshotInstanceId = snapshot.key();
				handleSnapshot(snapshot, new Object());
			}

			for (Integer instanceId : readInstanceIds) {
				if (snapshotInstanceId < instanceId)
					if (instances.get(instanceId).getState() == LogEntryState.DECIDED) {
						List<Request> requests = _paxos
								.extractValueList(instances.get(instanceId)
										.getValue());

						onRequestOrdered(instanceId, requests);
					}
			}
			_paxos.getStorage().updateFirstUncommitted();

			// TODO: check if this is correct
			onRecoveryFinished();
		}

		_logger.info("Recovery phase finished. Starting paxos protocol.");
		_paxos.startPaxos();

		int clientPort = _descriptor.getLocalProcess().getClientPort();
		IdGenerator idGenerator = new TimeBasedIdGenerator(_descriptor.localID,
				_descriptor.config.getN());

		// NS: Reduce the size of the logs.
		// IdGenerator idGenerator = new SimpleIdGenerator(_descriptor.localID,
		// _descriptor.config.getN());

		(new NioClientManager(clientPort, _commandCallback, idGenerator))
				.start();

		return storage;
	}

	private void onRecoveryFinished() {
		_dispatcher.execute(new Runnable() {
			@Override
			public void run() {
				// TODO: check if this is correct
				_recoveryPhase = false;
				_service.recoveryFinished();
			}
		});

	}

	private Storage createStorage() throws IOException {
		StableStorage stableStorage;
		Storage storage;
		switch (_crashModel) {
		case CrashStop:
			stableStorage = new UnstableStorage();
			storage = new SimpleStorage(stableStorage, _descriptor);
			if (stableStorage.getView() % storage.getN() == _descriptor.localID)
				stableStorage.setView(stableStorage.getView() + 1);
			return storage;
		case FullStableStorage:
			_recoveryPhase = true;
			_logger.info("Reading log from: " + _logPath);
			DiscWriter writer = new FullSSDiscWriter(_logPath);
			stableStorage = new SynchronousStableStorage(writer);
			storage = new SimpleStorage(stableStorage, _descriptor);
			if (stableStorage.getView() % storage.getN() == _descriptor.localID)
				stableStorage.setView(stableStorage.getView() + 1);
			return storage;
		}
		throw new RuntimeException(
				"Specified crash model is not implemented yet");
	}

	protected Paxos createPaxos(Storage storage) throws IOException {
		// TODO: cleaner way of choosing modular vs monolithich paxos
		// return new ModularPaxos(p.localID, processes, this, service, this);
		return new PaxosImpl(_descriptor, this, this, storage);
	}

	/**
	 * Sets the crash model handled by replica and paxos.
	 * 
	 * @param crashModel
	 *            the crash model to set
	 */
	public void setCrashModel(CrashModel crashModel) {
		_crashModel = crashModel;
	}

	/**
	 * Gets the recovery algorithm which will be used by replica and paxos.
	 * 
	 * @return the recoveryAlgorithm
	 */
	public CrashModel getCrashModel() {
		return _crashModel;
	}

	/**
	 * Sets the path to directory where all logs will be saved.
	 * 
	 * @param path
	 *            to directory where logs will be saved
	 */
	public void setLogPath(String path) {
		_logPath = path;
	}

	/**
	 * Gets the path to directory where all logs will be saved.
	 * 
	 * @return path
	 */
	public String getLogPath() {
		return _logPath;
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
			List<Request> requestByteArray;
			synchronized (_decidedWaitingExecution) {
				requestByteArray = _decidedWaitingExecution.remove(_executeUB);
			}
			if (requestByteArray == null)
				return;

			// list of executed request in this executeUB instance
			Vector<RequestId> requestIdList = new Vector<RequestId>();
			_executedDifference.put(_executeUB, requestIdList);

			for (Request request : requestByteArray) {
				Integer lastSequenceNumberFromClient = _executedRequests
						.get(request.getRequestId().getClientId());

				// prevents executing the same request few times.
				if (lastSequenceNumberFromClient != null
						&& request.getRequestId().getSeqNumber() <= lastSequenceNumberFromClient) {
					_logger
							.warning("Preventing duplicating message. Not executing "
									+ _executeUB + ", " + request);
					continue;
				}

				// add request to executed history
				requestIdList.add(request.getRequestId());

				byte[] result = _service
						.execute(request.getValue(), _executeUB);

				Reply reply = new Reply(request.getRequestId(), result);
				if (!BENCHMARK) {
					if (_logger.isLoggable(Level.INFO)) {
						// _logger.fine("Executed #" + _executeUB + ", id=" +
						// request.getRequestId() + ", req="
						// + request.getValue() + ", reply=" + result);
						_logger.info("Executed id=" + request.getRequestId());
					}
				}

				if (_logDecisions) {
					assert _decisionsLog != null : "Decision log cannot be null";
					_decisionsLog.println(_executeUB + ":"
							+ request.getRequestId());
				}

				_executedRequests.put(request.getRequestId().getClientId(),
						request.getRequestId().getSeqNumber());

				_commandCallback.handleReply(request, reply);
			}

			if (!BENCHMARK) {
				if (_logger.isLoggable(Level.INFO)) {
					_logger.info("Batch done " + _executeUB);
				}
			}
			// batching requests: inform the service that all requests assigned
			_service.instanceExecuted(_executeUB);

			_executeUB++;
		}
	}

	/** Called by the paxos box when a new request is ordered. */
	public void onRequestOrdered(int instance, List<Request> values) {
		if (_logger.isLoggable(Level.FINE)) {
			_logger.fine("Request ordered: " + instance + ":" + values);
		}

		// Simply notify the replica thread that a new request was ordered.
		// The replica thread will then try to execute
		synchronized (_decidedWaitingExecution) {
			_decidedWaitingExecution.put(instance, values);
		}

		_dispatcher.execute(new Runnable() {
			public void run() {
				executeDecided();
			}
		});

		if (instance > _paxos.getStorage().getFirstUncommitted()) {
			if (_logger.isLoggable(Level.INFO)) {
				_logger.info("Out of order decision. Expected: "
						+ (_paxos.getStorage().getFirstUncommitted()));
			}
		}
	}

	/*
	 * =================== Snapshot support ====================
	 */
	public void onSnapshotMade(final int instance, final byte[] snapshot) {
		_dispatcher.execute(new Runnable() {
			public void run() {
				// add header to snapshot
				Map<Long, Integer> requestHistory = new HashMap<Long, Integer>(
						_executedRequests);

				// update map to state in moment of snapshot
				for (int i = _executeUB - 1; i >= instance; i--) {
					List<RequestId> ids = _executedDifference.get(i);

					// this is null only when NoOp
					if (ids == null)
						continue;

					for (RequestId id : ids) {
						requestHistory.put(id.getClientId(),
								id.getSeqNumber() - 1);
					}
				}

				// save to byte array
				ByteBuffer buffer = ByteBuffer.allocate(snapshot.length + 4
						+ requestHistory.size() * (8 + 4));
				buffer.putInt(requestHistory.size());
				for (Entry<Long, Integer> entry : requestHistory.entrySet()) {
					buffer.putLong(entry.getKey());
					buffer.putInt(entry.getValue());
				}
				buffer.put(snapshot);
				assert buffer.remaining() == 0;

				_paxos.onSnapshotMade(instance, buffer.array());
			}
		});
	}

	public void handleSnapshot(final Pair<Integer, byte[]> snapshot,
			final Object _lock) {
		_logger.info("New snapshot received");
		_dispatcher.executeAndWait(new Runnable() {
			@Override
			public void run() {
				synchronized (_lock) {
					handleSnapshotInternal(snapshot);
				}
			}
		});
	}

	public void askForSnapshot(final int lastSnapshotInstance) {
		_dispatcher.execute(new Runnable() {
			public void run() {
				_logger.fine("Machine state asked for snapshot "
						+ lastSnapshotInstance);
				_service.askForSnapshot(lastSnapshotInstance);
			}
		});
	}

	public void forceSnapshot(final int lastSnapshotInstance) {
		_dispatcher.execute(new Runnable() {
			public void run() {
				_service.askForSnapshot(lastSnapshotInstance);
			}
		});
	}

	private void handleSnapshotInternal(Pair<Integer, byte[]> snapshot) {
		assert _dispatcher.amIInDispatcher();
		assert snapshot != null : "Snapshot is null";

		int firstUncommitted = _paxos.getStorage().getFirstUncommitted();

		if (!_recoveryPhase)
			if (snapshot.getKey() < firstUncommitted) {
				_logger
						.warning("Received snapshot is older than current state."
								+ snapshot
								+ ", firstUncommited: "
								+ firstUncommitted);
				return;
			}

		// read header (list of executed request contained in this snapshot)
		ByteBuffer buffer = ByteBuffer.wrap(snapshot.getValue());
		_executedRequests.clear();
		_executedDifference.clear();
		int size = buffer.getInt();
		for (int i = 0; i < size; i++) {
			Long clientId = buffer.getLong();
			Integer seqId = buffer.getInt();
			_executedRequests.put(clientId, seqId);
		}
		byte[] snapshotValue = new byte[buffer.remaining()];
		buffer.get(snapshotValue);

		_logger.info("Updating machine state from snapshot." + snapshot);
		_service.updateToSnapshot(snapshot.getKey(), snapshotValue);
		synchronized (_decidedWaitingExecution) {
			if (!_decidedWaitingExecution.isEmpty()) {
				if (_decidedWaitingExecution.lastKey() < snapshot.getKey())
					_decidedWaitingExecution.clear();
				else {
					while (_decidedWaitingExecution.firstKey() < snapshot
							.getKey()) {
						_decidedWaitingExecution.pollFirstEntry();
					}
				}
			}
			_executeUB = snapshot.getKey() + 1;
		}

		final Object _snapshotLock = new Object();

		synchronized (_snapshotLock) {
			_paxos.getDispatcher().dispatch(
					new AfterCatchupSnapshotEvent(snapshot,
							_paxos.getStorage(), _snapshotLock));

			try {
				_snapshotLock.wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		executeDecided();
	}

	public void forceExit() {
		_dispatcher.shutdownNow();
	}

	private final static Logger _logger = Logger.getLogger(Replica.class
			.getCanonicalName());
}
