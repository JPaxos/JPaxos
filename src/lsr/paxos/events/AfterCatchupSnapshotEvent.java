package lsr.paxos.events;

import lsr.paxos.Snapshot;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

public class AfterCatchupSnapshotEvent implements Runnable {

	private final Snapshot _snapshot;
	private final StableStorage _stableStorage;
	private final Storage _storage;
	private final Object snapshotLock;

	public AfterCatchupSnapshotEvent(Snapshot snapshot,
			Storage storage, final Object snapshotLock) {
		_snapshot = snapshot;
		this.snapshotLock = snapshotLock;
		_stableStorage = storage.getStableStorage();
		_storage = storage;
	}

	public void run() {

		int oldInstanceId = _stableStorage.getLastSnapshot().nextIntanceId;
		if (oldInstanceId >= _snapshot.nextIntanceId) {
			synchronized (snapshotLock) {
				snapshotLock.notify();
			}
			return;
		}

		_stableStorage.setLastSnapshot(_snapshot);
		_stableStorage.getLog().truncateBelow(oldInstanceId);
		_stableStorage.getLog().clearUndecidedBelow(_snapshot.nextIntanceId);
		_storage.updateFirstUncommitted();

		synchronized (snapshotLock) {
			snapshotLock.notify();
		}
	}
}
