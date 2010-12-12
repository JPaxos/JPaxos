package lsr.paxos.storage;

import lsr.paxos.Snapshot;

/**
 * Interface designed for stable storage, i.e. storage surviving crashes. In
 * opposite to Storage, all changes here must be back-noted providing access to
 * them after crash.
 */
public interface StableStorage {

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
     * Returns the Paxos Log.
     * 
     * @return the Paxos log
     */
    public Log getLog();

    /**
     * Returns the view number.
     * 
     * @return the view number
     */
    int getView();

    /**
     * Sets the view number.
     * 
     * @param view - the new view number
     */
    void setView(int view);
}
