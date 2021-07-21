package lsr.paxos.storage;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.paxos.UnBatcher;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

/**
 * A basic class implementing features needed by Paxos log. This class is not
 * using stable storage, that means all records are kept in RAM memory
 * exclusively!
 */
public class InMemoryLog implements Log {

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
    public InMemoryLog() {
        instances = new TreeMap<Integer, ConsensusInstance>();
    }

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#getInstanceMap()
     */
    @Override
    public SortedMap<Integer, ConsensusInstance> getInstanceMap() {
        return Collections.unmodifiableSortedMap(instances);
    }

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#getInstance(int)
     */
    @Override
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

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#append(int, byte[])
     */
    @Override
    public ConsensusInstance append() {
        ConsensusInstance instance = createInstance();
        instances.put(nextId, instance);
        nextId++;
        sizeChanged();
        return instance;
    }

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#getNextId()
     */
    @Override
    public int getNextId() {
        return nextId;
    }

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#getLowestAvailableId()
     */
    @Override
    public int getLowestAvailableId() {
        return lowestAvailable;
    }

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#truncateBelow(int)
     */
    @Override
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

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#clearUndecidedBelow(int)
     */
    @Override
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

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#addLogListener(lsr.paxos.storage.LogListener)
     */
    @Override
    public boolean addLogListener(LogListener listener) {
        return listeners.add(listener);
    }

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#removeLogListener(lsr.paxos.storage.LogListener)
     */
    @Override
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

    /* (non-Javadoc)
     * @see lsr.paxos.storage.ILog#byteSizeBetween(int, int)
     */
    @Override
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
        return new InMemoryConsensusInstance(nextId);
    }

    private final static Logger logger = LoggerFactory.getLogger(Log.class);
}
