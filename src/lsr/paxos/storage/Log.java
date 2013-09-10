package lsr.paxos.storage;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import lsr.paxos.UnBatcher;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic class implementing features needed by Paxos log. This class is not
 * using stable storage, that means all records are kept in RAM memory
 * exclusively!
 */
public class Log {

    /** Structure containing all kept instances */
    protected final TreeMap<Integer, ConsensusInstance> instances;

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
     * smallest id). All previous instances were truncated. The instance can be
     * of any state.
     * 
     * In some cases there actually are some instances below this point, but the
     * lower numbers are not contiguous. (See {@link #clearUndecidedBelow})
     * 
     * @return the id of lowest available instance
     */
    public int getLowestAvailableId() {
        return lowestAvailable;
    }

    /**
     * Removes instances with ID's strictly smaller than a given one. After
     * truncating the log, {@link #lowestAvailable} is updated.
     * 
     * @param instanceId - the id of consensus instance.
     * @return removed instances
     */
    public void truncateBelow(int instanceId) {
        assert instanceId >= lowestAvailable : "Cannot truncate below lower available.";

        lowestAvailable = instanceId;
        nextId = Math.max(nextId, lowestAvailable);

        ArrayList<ConsensusInstance> removed = new ArrayList<ConsensusInstance>();

        if (instances.isEmpty()) {
            return;
        }

        if (instanceId >= nextId) {
            removed.addAll(instances.values());
            instances.clear();
            if (processDescriptor.indirectConsensus)
                clearBatches(removed);
            return;
        }

        while (instances.firstKey() < instanceId) {
            removed.add((instances.pollFirstEntry().getValue()));
        }

        logger.debug("Truncated log below: {}", instanceId);

        if (processDescriptor.indirectConsensus)
            clearBatches(removed);
    }

    /**
     * Removes all undecided instances below given point. All instances with id
     * lower than <code>instanceId</code> and not decided will be removed from
     * the log.
     * 
     * @param instanceId - the id of consensus instance
     * @return removed instances
     */
    public void clearUndecidedBelow(int instanceId) {

        if (instances.size() == 0) {
            return;
        }

        lowestAvailable = instanceId;
        nextId = Math.max(nextId, lowestAvailable);

        ArrayList<ConsensusInstance> removed = new ArrayList<ConsensusInstance>();

        int first = instances.firstKey();
        for (int i = first; i < instanceId; i++) {
            ConsensusInstance instance = instances.get(i);
            if (instance != null && instance.getState() != LogEntryState.DECIDED) {
                removed.add(instances.remove(i));
            }
        }
        if (processDescriptor.indirectConsensus)
            clearBatches(removed);
    }

    private void clearBatches(ArrayList<ConsensusInstance> removed) {
        HashSet<ClientBatchID> cbids = new HashSet<ClientBatchID>();
        for (ConsensusInstance ci : removed) {
            if (ci.getValue() != null)
                cbids.addAll(UnBatcher.unpackCBID(ci.getValue()));
            ci.stopFwdBatchForwarder();
        }
        for (ConsensusInstance ci : instances.values()) {
            if (ci.getValue() != null)
                cbids.removeAll(UnBatcher.unpackCBID(ci.getValue()));
        }
        ClientBatchStore.instance.removeBatches(cbids);
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

    private final static Logger logger = LoggerFactory.getLogger(Log.class);
}
