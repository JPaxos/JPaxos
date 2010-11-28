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
    protected TreeMap<Integer, ConsensusInstance> instances;

    /** ID of next instance, that is highest instanceId + 1 */
    protected Integer nextId = 0;

    /** Lowest still held in memory instance number */
    protected Integer lowestAvailable = 0;

    /** List of objects to be informed about log changes */
    private List<LogListener> listeners = new Vector<LogListener>();

    /**  */
    public Log() {
        instances = new TreeMap<Integer, ConsensusInstance>();
    }

    /** Returns read-only access to the log */
    public SortedMap<Integer, ConsensusInstance> getInstanceMap() {
        return Collections.unmodifiableSortedMap(instances);
    }

    /** Returns, creating if needed, instance with provided ID */
    public ConsensusInstance getInstance(int instanceId) {
        Integer oldNextId = nextId;
        while (nextId <= instanceId) {
            instances.put(nextId, createInstance());
            nextId++;
        }
        if (oldNextId != nextId)
            sizeChanged();
        return instances.get(instanceId);
    }

    /** Returns, in a possibly thread-safe way, the state of given instance */
    public LogEntryState getState(int instanceId) {
        ConsensusInstance ci = instances.get(instanceId);
        if (ci == null) {
            if (instanceId < lowestAvailable)
                return LogEntryState.DECIDED;
            return LogEntryState.UNKNOWN;
        }
        return ci.getState();
    }

    /** Adds a new instance at the end of our list */
    public ConsensusInstance append(int view, byte[] value) {
        ConsensusInstance ci = createInstance(view, value);
        instances.put(nextId, ci);
        nextId++;
        sizeChanged();
        return ci;
    }

    public int getNextId() {
        return nextId;
    }

    public int getLowestAvailableId() {
        return lowestAvailable;
    }

    /** Removes instances with ID's strictly smaller than a given one */
    public void truncateBelow(int instanceId) {

        if (!ProcessDescriptor.getInstance().mayShareSnapshots) {
            return;
        }

        assert instanceId >= lowestAvailable : "Cannot truncate below lower available.";

        lowestAvailable = instanceId;
        nextId = Math.max(nextId, lowestAvailable);

        if (instances.size() == 0)
            return;

        if (instanceId >= nextId) {
            instances.clear();
            return;
        }

        while (instances.firstKey() < instanceId) {
            instances.pollFirstEntry();
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

        if (instances.size() == 0) {
            return;
        }

        lowestAvailable = key;
        nextId = Math.max(nextId, lowestAvailable);

        int first = instances.firstKey();
        for (int i = first; i < key; i++) {
            ConsensusInstance instance = instances.get(i);
            if (instance != null && instance.getState() != LogEntryState.DECIDED) {
                instances.remove(i);
            }
        }
    }

    public int getInstanceMapSize() {
        return instances.size();
    }

    public boolean addLogListener(LogListener listener) {
        return listeners.add(listener);
    }

    public boolean removeLogListener(LogListener listener) {
        return listeners.remove(listener);
    }

    /**
     * Calls function on all objects, that should be informed on log size change
     */
    protected void sizeChanged() {
        for (LogListener listener : listeners) {
            listener.logSizeChanged(instances.size());
        }
    }

    /**
     * Returns approximate size of log from 'startId' (inclusive) to 'endId'
     * (exclusive) in bytes.
     * 
     * That is, size of consensus instanced in range <startId, endId)
     */
    public long byteSizeBetween(int startId, int endId) {
        int start = Math.max(startId, instances.firstKey());
        int stop = Math.min(endId, nextId);
        long size = 0;
        ConsensusInstance current;
        for (int i = start; i < stop; ++i) {
            current = instances.get(i);
            if (current == null)
                continue;
            size += current.byteSize();
        }
        return size;
    }

    protected ConsensusInstance createInstance() {
        return new ConsensusInstance(nextId);
    }

    protected ConsensusInstance createInstance(int view, byte[] value) {
        return new ConsensusInstance(nextId, LogEntryState.KNOWN, view, value);
    }

    private final static Logger logger = Logger.getLogger(Log.class.getCanonicalName());

    public int size() {
        return instances.size();
    }
}
