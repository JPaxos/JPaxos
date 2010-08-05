package lsr.paxos.events;

import lsr.common.Pair;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

public class AfterCatchupSnapshotEvent implements Runnable {

	private final Pair<Integer, byte[]> _snapshot;
	private final StableStorage _stableStorage;
	private final Storage _storage;
	private final Object snapshotLock;

	public AfterCatchupSnapshotEvent(Pair<Integer, byte[]> snapshot, Storage storage, final Object snapshotLock) {
		_snapshot = snapshot;
		this.snapshotLock = snapshotLock;
		_stableStorage = storage.getStableStorage();
		_storage = storage;
	}

	public void run() {

		int oldInstanceId = _stableStorage.getLastSnapshotInstance();
		if (oldInstanceId >= _snapshot.getKey()) {
			synchronized (snapshotLock) {
				snapshotLock.notify();
			}
			return;
		}

		_stableStorage.setLastSnapshot(_snapshot);
		_stableStorage.getLog().truncateBelow(oldInstanceId);
		_stableStorage.getLog().clearUndecidedBelow(_snapshot.getKey());
		_storage.updateFirstUncommitted();

		synchronized (snapshotLock) {
			snapshotLock.notify();
		}
	}
}
