package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.Snapshot;

public class CatchUpSnapshot extends Message {

	private static final long serialVersionUID = 1L;

	/** Forwards the time of request, allowing dynamic timeouts for catch-up */
	private long _requestTime;

	private Snapshot _snapshot;

	public CatchUpSnapshot(int view, long requestTime,
			Snapshot snapshot) {
		super(view);

		_requestTime = requestTime;
		_snapshot = snapshot;
	}

	public CatchUpSnapshot(DataInputStream input) throws IOException {
		super(input);
		_requestTime = input.readLong();
		_snapshot = new Snapshot(input);
	}

	public void setRequestTime(long requestTime) {
		_requestTime = requestTime;
	}

	public long getRequestTime() {
		return _requestTime;
	}

	public void setSnapshot(Snapshot snapshot) {
		_snapshot = snapshot;
	}

	public Snapshot getSnapshot() {
		return _snapshot;
	}

	public MessageType getType() {
		return MessageType.CatchUpSnapshot;
	}

	protected void write(ByteBuffer bb) throws IOException {
		bb.putLong(_requestTime);
		_snapshot.appendToByteBuffer(bb);
	}

	public int byteSize() {
		return super.byteSize() + 8 + _snapshot.byteSize();
	}

	public String toString() {
		return "CatchUpSnapshot (" + super.toString() + ") up to request: "
				+ _snapshot.requestSeqNo;
	}
}
