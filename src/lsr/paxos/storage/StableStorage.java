package lsr.paxos.storage;

import lsr.paxos.Snapshot;

/**
 * Interface designed for stable storage, i.e. storage surviving crashes. In
 * opposite to Storage, all changes here must be back-noted providing access to
 * them after crash.
 */

public interface StableStorage {

    /**
     * Returns the last snapshot. A snapshot is a pair of integer (first
     * instance the snapshot has not yet done) and a serializable object
     */
    Snapshot getLastSnapshot();

    /** Sets the last snapshot to given value */
    void setLastSnapshot(Snapshot snapshot);

    /** Returns the Paxos Log */
    public Log getLog();

    int getView();

    void setView(int view);
}
