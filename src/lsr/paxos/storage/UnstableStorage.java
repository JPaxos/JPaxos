package lsr.paxos.storage;

import lsr.common.Pair;

public class UnstableStorage implements StableStorage {
	protected int _view;
	protected Log _log;
	private Pair<Integer, byte[]> _lastSnapshot;

	public UnstableStorage() {
		_log = new Log();
	}

	public UnstableStorage(Log log) {
		_log = log;
	}

	public Log getLog() {
		return _log;
	}

	public Pair<Integer, byte[]> getLastSnapshot() {
		return _lastSnapshot;
	}

	public void setLastSnapshot(Pair<Integer, byte[]> snapshot) {
		assert _lastSnapshot == null
				|| _lastSnapshot.getKey() <= snapshot.getKey();
		_lastSnapshot = snapshot;
	}

	/** Returns ID of first not snapshotted instance */
	public int getLastSnapshotInstance() {
		if (_lastSnapshot == null)
			return 0;
		return _lastSnapshot.getKey();
	}

	public int getView() {
		return _view;
	}

	public void setView(int view) throws IllegalArgumentException {
		if (view <= _view)
			throw new IllegalArgumentException(
					"Cannot set smaller or equal view.");
		_view = view;
	}
}
