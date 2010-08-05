package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Alive extends Message {
	private static final long serialVersionUID = 1L;
	/**
	 * LogSize is the size of log (== the highest started instanceID) of the
	 * leader
	 */
	private int _logSize;

	public Alive(int view, int logSize) {
		super(view);
		_logSize = logSize;
	}

	public Alive(DataInputStream input) throws IOException {
		super(input);
		_logSize = input.readInt();
	}

	public int getLogSize() {
		return _logSize;
	}

	public MessageType getType() {
		return MessageType.Alive;
	}

//	protected void write(DataOutputStream os) throws IOException {
//		os.writeInt(_logSize);
//	}

	public int byteSize() {
		return super.byteSize() + 4;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _logSize;
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Alive other = (Alive) obj;
		if (_logSize != other._logSize)
			return false;
		return true;
	}

	public String toString() {
		return "ALIVE (" + super.toString() + ", logsize: " + _logSize + ")";
	}

	@Override
	protected void write(ByteBuffer bb) throws IOException {
		bb.putInt(_logSize);
	}
}
