package lsr.paxos.replica;

import lsr.paxos.Snapshot;
import lsr.service.Service;

/**
 * A SnapshotListener is informed that a snapshot has been done. He also
 * receives the snapshot immediately.
 */

public interface SnapshotListener2 {
    /**
     * Called by {@link Service} when a new snapshot has been made.
     * 
     * @param snapshot the value of snapshot
     */
    void onSnapshotMade(Snapshot snapshot);
}
