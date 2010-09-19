package lsr.paxos.storage;

import lsr.paxos.Snapshot;

public class UnstableStorage implements StableStorage {
	protected int _view;
	protected Log _log;
	private Snapshot _lastSnapshot;

	public UnstableStorage() {
		_log = new Log();
	}

	public UnstableStorage(Log log) {
		_log = log;
	}

	public Log getLog() {
		return _log;
	}

	public Snapshot getLastSnapshot() {
		return _lastSnapshot;
	}

	public void setLastSnapshot(Snapshot snapshot) {
		assert _lastSnapshot == null || _lastSnapshot.compare(snapshot) <= 0;
		_lastSnapshot = snapshot;
	}

	public int getView() {
		return _view;
	}

	public void setView(int view) throws IllegalArgumentException {
		if (view <= _view)
			throw new IllegalArgumentException("Cannot set smaller or equal view.");
		_view = view;
	}
}
