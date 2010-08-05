package lsr.paxos;

/**
 * A SnapshotListener is informed that a snapshot has been done. He also
 * receives the snapshot immediately.
 */

public interface SnapshotListener {
	void onSnapshotMade(int instance, byte[] snapshot);
}
