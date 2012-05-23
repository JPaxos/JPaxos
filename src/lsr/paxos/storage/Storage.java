package lsr.paxos.storage;

import java.util.BitSet;

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
	
	void setFirstUncommitted(int firstUncommitted);

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
    
    int getWindowUsed();
    
    /** returns true if the window is full, \ie, we reached maximum number 
     * of open parallel instances  
     */
    boolean isWindowFull();

    /**
     * Returns true if there are no undecided consensus instances.
     * 
     * @return true if there are no undecided consensus instances
     */
    public boolean isIdle();

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
     * @throws IllegalArgumentException if new view is lower or equal than
     *             actual one
     */
    void setView(int view);

    /**
     * Updates the epoch vector inside this storage by taking maximum from
     * specified epoch and epoh inside this storage.
     * 
     * <p>
     * The length of given epoch has to match the epoch vector inside this
     * storage.
     * <p>
     * Example:
     * 
     * <pre>
     * storage.setEpoch(new long[] {1, 2, 3});
     * storage.updateEpoch(new long[] {2, 1, 4});
     * storage.getEpoch(); // {2, 2, 4}
     * </pre>
     * 
     * @param epoch - the new epoch vector
     * @throws IllegalArgumentException when size of epoch is different than one
     *             stored inside this class
     */
    void updateEpoch(long[] epoch);

    /**
     * Updates the epoch for one process inside this storage by taking maximum
     * from specified epoch and epoch inside this storage.
     * 
     * <p>
     * The length of give epoch has to match the epoch vector inside this
     * storage
     * <p>
     * Example:
     * 
     * <pre>
     * storage.setEpoch(new long[] {1, 2, 3});
     * storage.updateEpoch(2, 0);
     * storage.getEpoch(); // {2, 2, 3}
     * </pre>
     * 
     * @param epoch - the new epoch vector
     * @throws IllegalArgumentException when sender is less than zero or greater
     *             or equal than size of current epoch
     */
    void updateEpoch(long epoch, int sender);
}
