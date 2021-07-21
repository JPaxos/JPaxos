package lsr.paxos.storage;

import java.util.SortedMap;

public interface Log {

    /** Returns read-only access to the log */
    SortedMap<Integer, ? extends ConsensusInstance> getInstanceMap();

    /** Returns, creating if needed, instance with provided ID */
    ConsensusInstance getInstance(int instanceId);

    /**
     * Adds a new instance at the end of the log.
     * 
     * @param view - the view of new consensus instance
     * @param value - the value of new consensus instance
     * @return new consensus log
     */
    ConsensusInstance append();

    /**
     * Returns the id of next consensus instance. The id of highest instance
     * stored in the log is equal to <code>getNextId() - 1</code>.
     * 
     * @return the id of next consensus instance
     */
    int getNextId();

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
    int getLowestAvailableId();

    /**
     * Removes instances with ID's strictly smaller than a given one. After
     * truncating the log, {@link #lowestAvailable} is updated.
     * 
     * @param instanceId - the id of consensus instance.
     * @return removed instances
     */
    void truncateBelow(int instanceId);

    /**
     * Removes all undecided instances below given point. All instances with id
     * lower than <code>instanceId</code> and not decided will be removed from
     * the log.
     * 
     * @param instanceId - the id of consensus instance
     * @return removed instances
     */
    void clearUndecidedBelow(int instanceId);

    /**
     * Registers specified listener. This listener will be called every time
     * this log has been changed.
     * 
     * @param listener - the listener to register
     * @return true if the listener has been registered.
     */
    boolean addLogListener(LogListener listener);

    /**
     * Unregisters specified listener from the log.
     * 
     * @param listener - the listener that will be removed
     * @return true if the listener was already registered
     */
    boolean removeLogListener(LogListener listener);

    /**
     * Returns approximate size of log from 'startId' (inclusive) to 'endId'
     * (exclusive) in bytes.
     * 
     * That is, size of consensus instances in range <startId, endId).
     * 
     * @return size of log in bytes
     */
    long byteSizeBetween(int startId, int endId);

}