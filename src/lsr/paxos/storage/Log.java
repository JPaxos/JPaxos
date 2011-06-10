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

    // This field is read from other threads (eg., ActiveFailureDetector), 
    // therefore must be made volatile to ensure visibility of changes
    /** ID of next instance, that is highest instanceId + 1 */
    protected volatile int nextId = 0;

    /** Lowest still held in memory instance number */
    protected Integer lowestAvailable = 0;

    /** List of objects to be informed about log changes */
    private List<LogListener> listeners = new Vector<LogListener>();

    /**
     * Creates new instance of empty <code>Log</code>.
     */
    public Log() {
        instances = new TreeMap<Integer, ConsensusInstance>();
    }

    /** Returns read-only access to the log */
    public SortedMap<Integer, ConsensusInstance> getInstanceMap() {
        return Collections.unmodifiableSortedMap(instances);
    }

    /** Returns, creating if needed, instance with provided ID */
    public ConsensusInstance getInstance(int instanceId) {
        int oldNextId = nextId;
        while (nextId <= instanceId) {
            instances.put(nextId, createInstance());
            nextId++;
        }
        if (oldNextId != nextId) {
            sizeChanged();
        }
        return instances.get(instanceId);
    }

    /**
     * Adds a new instance at the end of the log.
     * 
     * @param view - the view of new consensus instance
     * @param value - the value of new consensus instance
     * @return new consensus log
     */
    public ConsensusInstance append(int view, byte[] value) {
        ConsensusInstance instance = createInstance(view, value);
        instances.put(nextId, instance);
        nextId++;
        sizeChanged();
        return instance;
    }

    /**
     * Returns the id of next consensus instance. The id of highest instance
     * stored in the log is equal to <code>getNextId() - 1</code>.
     * 
     * @return the id of next consensus instance
     */
    public int getNextId() {
        return nextId;
    }

    /**
     * Returns the id of lowest available instance (consensus instance with the
     * smallest id).
     * 
     * TODO TZ - is it lowest decided instance or any available?
     * 
     * @return the id of lowest available instance
     */
    public int getLowestAvailableId() {
        return lowestAvailable;
    }

    /**
     * Removes instances with ID's strictly smaller than a given one. After
     * truncating the log, <code>lowestAvailableId</code> is updated.
     * 
     * @param instanceId - the id of consensus instance.
     */
    public void truncateBelow(int instanceId) {

        if (!ProcessDescriptor.getInstance().mayShareSnapshots) {
            return;
        }

        assert instanceId >= lowestAvailable : "Cannot truncate below lower available.";

        lowestAvailable = instanceId;
        nextId = Math.max(nextId, lowestAvailable);

        if (instances.size() == 0) {
            return;
        }

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

    /**
     * Removes all undecided instances below given point. All instances with id
     * lower than <code>instanceId</code> and not decided will be removed from
     * the log.
     * 
     * @param instanceId - the id of consensus instance
     */
    public void clearUndecidedBelow(int instanceId) {

        if (!ProcessDescriptor.getInstance().mayShareSnapshots) {
            return;
        }

        if (instances.size() == 0) {
            return;
        }

        lowestAvailable = instanceId;
        nextId = Math.max(nextId, lowestAvailable);

        int first = instances.firstKey();
        for (int i = first; i < instanceId; i++) {
            ConsensusInstance instance = instances.get(i);
            if (instance != null && instance.getState() != LogEntryState.DECIDED) {
                instances.remove(i);
            }
        }
    }

    /**
     * Registers specified listener. This listener will be called every time
     * this log has been changed.
     * 
     * @param listener - the listener to register
     * @return true if the listener has been registered.
     */
    public boolean addLogListener(LogListener listener) {
        return listeners.add(listener);
    }

    /**
     * Unregisters specified listener from the log.
     * 
     * @param listener - the listener that will be removed
     * @return true if the listener was already registered
     */
    public boolean removeLogListener(LogListener listener) {
        return listeners.remove(listener);
    }

    /**
     * Calls function on all objects, that should be informed on log size
     * change.
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
     * That is, size of consensus instances in range <startId, endId).
     * 
     * @return size of log in bytes
     */
    public long byteSizeBetween(int startId, int endId) {
        int start = Math.max(startId, instances.firstKey());
        int stop = Math.min(endId, nextId);
        long size = 0;
        ConsensusInstance current;
        for (int i = start; i < stop; ++i) {
            current = instances.get(i);
            if (current == null) {
                continue;
            }
            size += current.byteSize();
        }
        return size;
    }

    /**
     * Returns number of instances stored in the log. If snapshot was made by
     * Paxos protocol and some instances have been truncated, the size will be
     * different than <code>getNextId()</code>.
     * 
     * For example if log contains instances 5 - 10, because instances below 5
     * have been truncated, this method will return 6.
     * 
     * @return number of instances stored in the log
     */
    public int size() {
        return instances.size();
    }

    /**
     * Creates new empty consensus instance.
     * 
     * @return new consensus instance.
     */
    protected ConsensusInstance createInstance() {
        return new ConsensusInstance(nextId);
    }

    /**
     * Creates new consensus instance with specified view and value.
     * 
     * @param view - the view number
     * @param value - the value
     * @return new consensus instance
     */
    protected ConsensusInstance createInstance(int view, byte[] value) {
        return new ConsensusInstance(nextId, LogEntryState.KNOWN, view, value);
    }

    private final static Logger logger = Logger.getLogger(Log.class.getCanonicalName());
}
