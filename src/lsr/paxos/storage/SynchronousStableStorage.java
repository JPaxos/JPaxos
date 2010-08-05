package lsr.paxos.storage;

import java.io.IOException;

public class SynchronousStableStorage extends UnstableStorage {
	private final DiscWriter _writer;

	public SynchronousStableStorage(DiscWriter writer) throws IOException {
		_view = writer.loadViewNumber();
		_writer = writer;
		_log = new SynchronousLog(writer);
	}

	@Override
	public void setView(int view) throws IllegalArgumentException {
		_writer.changeViewNumber(view);
		super.setView(view);
	}
}
