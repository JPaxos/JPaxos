package lsr.paxos.storage;

import java.util.BitSet;

import lsr.paxos.Snapshot;

/**
 * Represents the storage with state of the Paxos protocol like view number,
 * list of consensus instances and snapshots.
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
     * Returns set of acceptors.
     * 
     * @return set of acceptors
     */
    BitSet getAcceptors();

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

    /**
     * Returns the last snapshot.
     * 
     * @return the last snapshot
     */
    Snapshot getLastSnapshot();

    /**
     * Sets the last snapshot to given value.
     * 
     * @param snapshot - the new snapshot
     */
    void setLastSnapshot(Snapshot snapshot);

    /**
     * Returns the view number.
     * 
     * @return the view number
     */
    int getView();

    /**
     * Returns the epoch array - known epoch number of every process.
     * 
     * @return the array with epoch numbers
     */
    long[] getEpoch();

    /**
     * Sets the new epoch array.
     * 
     * @param epoch - the new epoch array
     */
    void setEpoch(long[] epoch);

    /**
     * Sets the view number. The new view number has to be greater than actual
     * one.
     * 
     * @param view - the new view number
     * @throws IllegalArgumentException - if new view is lower or equal than
     *             actual one
     */
    void setView(int view);

    // TODO TZ - add comment
    void updateEpoch(long[] epoch);

    void updateEpoch(long epoch, int sender);
}
