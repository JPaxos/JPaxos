package lsr.paxos.storage;

import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.paxos.Batcher;
import lsr.paxos.BatcherImpl;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * A basic class implementing features needed by Paxos log. This class is not
 * using stable storage, that means all records are kept in RAM memory
 * exclusively!
 */
public class Log implements PublicLog {

	/** Structure containing all kept instances */
	protected TreeMap<Integer, ConsensusInstance> _instances;

	/** ID of next instance, that is highest instanceId + 1 */
	protected Integer _nextId = 0;

	/** Lowest still held in memory instance number */
	protected Integer _lowestAvailable = 0;

	/** List of objects to be informed about log changes */
	private List<LogListener> _listeners = new Vector<LogListener>();

	/** Number of requests forwarded to service */
	private int _executeSeqNo;

	/**  */
	public Log() {
		_instances = new TreeMap<Integer, ConsensusInstance>();
	}

	/** Returns read-only access to the log */
	public SortedMap<Integer, ConsensusInstance> getInstanceMap() {
		return Collections.unmodifiableSortedMap(_instances);
	}

	/** Returns, creating if needed, instance with provided ID */
	public ConsensusInstance getInstance(int instanceId) {
		Integer oldNextId = _nextId;
		while (_nextId <= instanceId) {
			_instances.put(_nextId, createInstance());
			_nextId++;
		}
		if (oldNextId != _nextId)
			sizeChanged();
		return _instances.get(instanceId);
	}

	/** Returns, in a possibly thread-safe way, the state of given instance */
	public LogEntryState getState(int instanceId) {
		ConsensusInstance ci = _instances.get(instanceId);
		if (ci == null) {
			if (instanceId < _lowestAvailable)
				return LogEntryState.DECIDED;
			return LogEntryState.UNKNOWN;
		}
		return ci.getState();
	}

	/** Adds a new instance at the end of our list */
	public ConsensusInstance append(int view, byte[] value) {
		ConsensusInstance ci = createInstance(view, value);
		_instances.put(_nextId, ci);
		_nextId++;
		sizeChanged();
		return ci;
	}

	public int getNextId() {
		return _nextId;
	}

	public int getLowestAvailableId() {
		return _lowestAvailable;
	}

	/** Removes instances with ID's strictly smaller than a given one */
	public void truncateBelow(int instanceId) {

		if (!ProcessDescriptor.getInstance().mayShareSnapshots) {
			return;
		}

		assert instanceId >= _lowestAvailable : "Cannot truncate below lower available.";

		_lowestAvailable = instanceId;
		_nextId = Math.max(_nextId, _lowestAvailable);

		if (_instances.size() == 0)
			return;

		if (instanceId >= _nextId) {
			_instances.clear();
			return;
		}

		while (_instances.firstKey() < instanceId) {
			_instances.pollFirstEntry();
		}

		if (logger.isLoggable(Level.FINE)) {
			logger.fine("Truncated log below: " + instanceId);
		}
	}

	/** Removes all undecided instances below given point */
	public void clearUndecidedBelow(Integer key) {

		if (!ProcessDescriptor.getInstance().mayShareSnapshots) {
			return;
		}

		if (_instances.size() == 0) {
			return;
		}

		_lowestAvailable = key;
		_nextId = Math.max(_nextId, _lowestAvailable);

		int first = _instances.firstKey();
		for (int i = first; i < key; i++) {
			ConsensusInstance instance = _instances.get(i);
			if (instance != null && instance.getState() != LogEntryState.DECIDED) {
				_instances.remove(i);
			}
		}
	}

	public int getInstanceMapSize() {
		return _instances.size();
	}

	public boolean addLogListener(LogListener listener) {
		return _listeners.add(listener);
	}

	public boolean removeLogListener(LogListener listener) {
		return _listeners.remove(listener);
	}

	/**
	 * Calls function on all objects, that should be informed on log size change
	 */
	protected void sizeChanged() {
		for (LogListener listener : _listeners) {
			listener.logSizeChanged(_instances.size());
		}
	}

	/**
	 * Returns approximate size of log from 'startId' (inclusive) to 'endId'
	 * (exclusive) in bytes.
	 * 
	 * That is, size of consensus instanced in range <startId, endId)
	 */
	public long byteSizeBetween(int startId, int endId) {
		int start = Math.max(startId, _instances.firstKey());
		int stop = Math.min(endId, _nextId);
		long size = 0;
		ConsensusInstance current;
		for (int i = start; i < stop; ++i) {
			current = _instances.get(i);
			if (current == null)
				continue;
			size += current.byteSize();
		}
		return size;
	}

	protected ConsensusInstance createInstance() {
		return new ConsensusInstance(_nextId);
	}

	protected ConsensusInstance createInstance(int view, byte[] value) {
		return new ConsensusInstance(_nextId, LogEntryState.KNOWN, view, value);
	}

	private final static Logger logger = Logger.getLogger(Log.class.getCanonicalName());

	public int size() {
		return _instances.size();
	}

	// /////// External access to logs //////// //

	// TODO: JK get the ProcesDescriptor.batchingLevel here
	private Batcher _batcher = new BatcherImpl(ProcessDescriptor.getInstance().batchingLevel);

	/** Records how many requests were executed on the state machine */
	public void setHighestExecuteSeqNo(int executeSeqNo) {
		_executeSeqNo = executeSeqNo;
	}

	/** Informs how many requests were executed on the state machine */
	public int getHighestExecuteSeqNo() {
		return _executeSeqNo;
	}

	/** Retrieves a specific request */
	public byte[] getRequest(int requestNo) {
		Integer instanceId = getInstanceForSeqNo(requestNo);

		if (instanceId == null)
			return null;

		int seqNo = _instances.get(instanceId).getStartingExecuteSeqNo();

		// Retrieving the instance and request

		Deque<Request> values = _batcher.unpack(_instances.get(instanceId).getValue());

		BitSet bs = _instances.get(instanceId).getExecuteMarker();
		while (seqNo++ != requestNo)
			bs.clear(bs.nextSetBit(0));

		if (bs.nextSetBit(0) != -1)
			return values.toArray(new Request[values.size()])[bs.nextSetBit(0)].getValue();

		return null;
	}

	/**
	 * Returns the instance containing given requestNo
	 * 
	 * @return null if instance is not available
	 * @return instance ID
	 */
	public Integer getInstanceForSeqNo(int requestNo) {

		if (_nextId == _lowestAvailable) {
			logger.fine("Requested instance for a seqNo when log is empty");
			return null;
		}

		if (_instances.get(_lowestAvailable).getStartingExecuteSeqNo() > requestNo) {
			logger.warning("Requested instance for a seqNo that has been truncated");
			return null;
		}

		if (requestNo > _executeSeqNo) {
			logger.warning("Requested instance for a seqNo that hes not yet been executed");
			return null;
		}

		// First kept executed instance
		int startSearch = _lowestAvailable;
		int startVal = _instances.get(startSearch).getStartingExecuteSeqNo();

		// Last kept executed instance
		int stopSearch = _nextId - 1;
		int stopVal = _instances.get(stopSearch).getStartingExecuteSeqNo();

		while (stopVal == -1) {
			assert startSearch < stopSearch;
			stopSearch--;
			stopVal = _instances.get(stopSearch).getStartingExecuteSeqNo();
		}

		if (stopVal <= requestNo)
			return stopSearch;

		// 'Binary' search for instanceID

		int middle;
		int middleVal;

		while (stopSearch > startSearch + 1) {

			if (startVal == stopVal) {
				startSearch = stopSearch;
				break;
			}

			middle = (int) (startSearch + (stopSearch - startSearch)
					* ((requestNo - startVal) / ((double) stopVal - startVal)));

			if (middle <= startSearch)
				middle = startSearch + 1;

			if (middle >= stopSearch)
				middle = stopSearch - 1;

			middleVal = _instances.get(middle).getStartingExecuteSeqNo();

			if (middleVal > requestNo) {
				stopSearch = middle;
				stopVal = middleVal;
			} else {
				startSearch = middle;
				startVal = middleVal;
			}

		}

		if (stopVal == requestNo) {
			startSearch = stopSearch;
		}
		return startSearch;
	}

	/**
	 * Returns all available executed requests <b>This may be dangerous, as the
	 * number of requests may be huge</b>
	 */
	public SortedMap<Integer, byte[]> getRequests() {
		return getRequests(0, _executeSeqNo);
	}

	/** Returns specific range of requests */
	public SortedMap<Integer, byte[]> getRequests(int startingNo, int finishingNo) {

		SortedMap<Integer, byte[]> map = new TreeMap<Integer, byte[]>();

		for (int i = _instances.size() - 1; i >= 0; --i) {
			if (_instances.get(i).getStartingExecuteSeqNo() == -1)
				continue;
			if (_instances.get(i).getStartingExecuteSeqNo() > finishingNo)
				continue;

			int seqNo = _instances.get(i).getStartingExecuteSeqNo();
			BitSet bs = _instances.get(i).getExecuteMarker();

			Deque<Request> d = _batcher.unpack(_instances.get(i).getValue());

			for (int j = 0; j < d.size(); j++) {
				byte[] value = d.poll().getValue();
				if (bs.get(j)) {
					map.put(seqNo, value);
					seqNo++;
				}
			}
			if (_instances.get(i).getStartingExecuteSeqNo() < startingNo)
				break;
		}

		while (!map.isEmpty() && map.firstKey() < startingNo)
			map.remove(map.firstKey());

		while (!map.isEmpty() && map.lastKey() > finishingNo)
			map.remove(map.lastKey());

		return map;
	}

}
