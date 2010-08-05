package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.common.Pair;

public class CatchUpSnapshot extends Message {

	private static final long serialVersionUID = 1L;

	/** Forwards the time of request, allowing dynamic timeouts for catch-up */
	private long _requestTime;

	private Pair<Integer, byte[]> _snapshot;

	public CatchUpSnapshot(int view, long requestTime, Pair<Integer, byte[]> snapshot) {
		super(view);

		_requestTime = requestTime;
		_snapshot = snapshot;
	}

	public CatchUpSnapshot(DataInputStream input) throws IOException {
		super(input);
		_requestTime = input.readLong();
		int instanceId = input.readInt();
		_snapshot = new Pair<Integer, byte[]>(instanceId, new byte[input.readInt()]);
		input.readFully(_snapshot.value());
	}

	public void setRequestTime(long requestTime) {
		_requestTime = requestTime;
	}

	public long getRequestTime() {
		return _requestTime;
	}

	public void setSnapshot(Pair<Integer, byte[]> snapshot) {
		_snapshot = snapshot;
	}

	public Pair<Integer, byte[]> getSnapshot() {
		return _snapshot;
	}

	public MessageType getType() {
		return MessageType.CatchUpSnapshot;
	}

//	protected void write(DataOutputStream os) throws IOException {
//		os.writeLong(_requestTime);
//		os.writeInt(_snapshot.key());
//		os.writeInt(_snapshot.value().length);
//		os.write(_snapshot.value());
//	}
	
	protected void write(ByteBuffer bb) throws IOException {
		bb.putLong(_requestTime);
		bb.putInt(_snapshot.key());
		bb.putInt(_snapshot.value().length);
		bb.put(_snapshot.value());
	}
	
	public int byteSize() {
		return super.byteSize() + 8 + 4 + 4 + _snapshot.value().length;
	}

	public String toString() {
		return "CatchUpSnapshot (" + super.toString() + ") up to instance: " + _snapshot.getKey();
	}
}
