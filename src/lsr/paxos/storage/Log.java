package lsr.paxos.storage;

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * A basic class implementing features needed by Paxos log. This class is not
 * using stable storage, that means all records are kept in RAM memory
 * exclusively!
 */
public class Log {

	/** Structure containing all kept instances */
	protected TreeMap<Integer, ConsensusInstance> _instances;

	/** ID of next instance, that is highest instanceId + 1 */
	protected Integer _nextId = 0;

	/** Lowest still held in memory instance number */
	protected Integer _lowestAvailable = 0;

	/** List of objects to be informed about log changes */
	private List<LogListener> _listeners = new Vector<LogListener>();

	// /** Estimate of the size of the log */
	// private int sizeInBytes = 0;

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
			if (instance != null
					&& instance.getState() != LogEntryState.DECIDED) {
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

	private final static Logger logger = Logger.getLogger(Log.class
			.getCanonicalName());

	public int size() {
		return _instances.size();
	}
}
