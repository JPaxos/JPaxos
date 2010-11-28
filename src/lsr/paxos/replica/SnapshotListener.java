package lsr.paxos.replica;

import lsr.service.Service;

/**
 * A SnapshotListener is informed that a snapshot has been done. He also
 * receives the snapshot immediately.
 */

public interface SnapshotListener {
    /**
     * Called by {@link Service} when a new snapshot has been made.
     * 
     * @param requestSeqNo ordinal number of last executed request
     * @param snapshot the value of snapshot
     */
    void onSnapshotMade(int requestSeqNo, byte[] snapshot, byte[] response);
}
