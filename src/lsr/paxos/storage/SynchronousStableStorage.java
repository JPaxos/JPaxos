package lsr.paxos.storage;

import java.io.IOException;

import lsr.common.Pair;

public class SynchronousStableStorage extends UnstableStorage {
	public final DiscWriter _writer;

	public SynchronousStableStorage(DiscWriter writer) throws IOException {
		_view = writer.loadViewNumber();
		_writer = writer;

		// Synchronous log reads the previous log files
		_log = new SynchronousLog(writer);

		Pair<Integer, byte[]> snapshot = _writer.getSnapshot();
		if (snapshot != null) {
			super.setLastSnapshot(snapshot);
		}
	}

	@Override
	public void setView(int view) throws IllegalArgumentException {
		_writer.changeViewNumber(view);
		super.setView(view);
	}

	@Override
	public void setLastSnapshot(Pair<Integer, byte[]> snapshot) {
		_writer.newSnapshot(snapshot);
		super.setLastSnapshot(snapshot);
	}
}
