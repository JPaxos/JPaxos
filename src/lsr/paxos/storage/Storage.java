package lsr.paxos.storage;

import java.util.BitSet;

/**
 * Represents the storage with state of the Paxos protocol like view number,
 * list of consensus instances, etc.
 */
public interface Storage {
    /**
     * Updates value of first undecided instance id. This method should be
     * called every time log has been updated.
     */
    void updateFirstUncommitted();

    /**
     * Returns first consensus instance for which there is yet no decision. That
     * is, <code>topDecided + 1</code> is the first instance for which this
     * process doesn't know the decision.
     * 
     * @return the id of first undecided instance
     */
    int getFirstUncommitted();

    /**
     * Returns number of processes.
     * 
     * @return number of processes
     */
    int getN();

    /**
     * Returns the id of local replica. The id of replica is always between 0
     * (inclusive) and <code>getN()</code> (exclusive).
     * 
     * @return the id of local replica
     */
    int getLocalId();

    /**
     * Returns set of acceptors.
     * 
     * @return set of acceptors
     */
    BitSet getAcceptors();

    /**
     * Returns the stable storage instance.
     * 
     * @return the stable storage instance
     */
    StableStorage getStableStorage();

    /**
     * Returns the log from paxos protocol, containing list of consensus
     * instances.
     * 
     * @return the log
     */
    Log getLog();

    /**
     * Returns true if the instance is inside a window.
     * 
     * @param instanceId - the id of consensus instance
     * @return true if the consensus instance id is inside a window
     */
    boolean isInWindow(int instanceId);

    /**
     * Returns true if there are no undecided consensus instances.
     * 
     * @return true if there are no undecided consensus instances
     */
    public boolean isIdle();
}
