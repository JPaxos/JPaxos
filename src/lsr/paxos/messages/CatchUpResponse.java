package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import lsr.paxos.storage.ConsensusInstance;

/**
 * Represents the catch-up mechanism response message
 */

public class CatchUpResponse extends Message {

	private static final long serialVersionUID = 1L;

	/**
	 * List of all requested instances, which were decided by the sender
	 */
	private List<ConsensusInstance> _decided;

	/** Forwards the time of request, allowing dynamic timeouts for catch-up */
	private long _requestTime;

	private boolean _haveSnapshotOnly = false;

	private boolean _periodicQuery = false;

	private boolean _isLastPart = true;

	public CatchUpResponse(int view, long requestTime,
			List<ConsensusInstance> decided) {
		super(view);
		// Create a copy
		_decided = new ArrayList<ConsensusInstance>(decided);
		_requestTime = requestTime;
	}

	public CatchUpResponse(DataInputStream input) throws IOException {
		super(input);
		byte flags = input.readByte();
		_periodicQuery = (flags & 1) == 0 ? false : true;
		_haveSnapshotOnly = (flags & 2) == 0 ? false : true;
		_isLastPart = (flags & 4) == 0 ? false : true;
		_requestTime = input.readLong();

		_decided = new Vector<ConsensusInstance>();
		for (int i = input.readInt(); i > 0; --i) {
			_decided.add(new ConsensusInstance(input));
		}
	}

	public void setDecided(List<ConsensusInstance> decided) {
		_decided = decided;
	}

	public List<ConsensusInstance> getDecided() {
		return _decided;
	}

	public void setRequestTime(long requestTime) {
		this._requestTime = requestTime;
	}

	public long getRequestTime() {
		return _requestTime;
	}

	public void setSnapshotOnly(boolean haveSnapshotOnly) {
		_haveSnapshotOnly = haveSnapshotOnly;
	}

	public boolean isSnapshotOnly() {
		return _haveSnapshotOnly;
	}

	public void setPeriodicQuery(boolean periodicQuery) {
		_periodicQuery = periodicQuery;
	}

	public boolean isPeriodicQuery() {
		return _periodicQuery;
	}

	public void setLastPart(boolean isLastPart) {
		_isLastPart = isLastPart;
	}

	public boolean isLastPart() {
		return _isLastPart;
	}

	public MessageType getType() {
		return MessageType.CatchUpResponse;
	}

	@Override
	protected void write(ByteBuffer bb) throws IOException {
		bb
				.put((byte) ((_periodicQuery ? 1 : 0)
						+ (_haveSnapshotOnly ? 2 : 0) + (_isLastPart ? 4 : 0)));
		bb.putLong(_requestTime);
		bb.putInt(_decided.size());
		for (ConsensusInstance ci : _decided) {
			ci.write(bb);
		}
	}

	@Override
	public int byteSize() {
		int sz = super.byteSize() + 1 + 8 + 4;
		for (ConsensusInstance ci : _decided) {
			sz += ci.byteSize();
		}
		return sz;
	}

	// protected void write(DataOutputStream os) throws IOException {
	// os.writeByte((_periodicQuery ? 1 : 0) + (_haveSnapshotOnly ? 2 : 0) +
	// (_isLastPart ? 4 : 0));
	// os.writeLong(_requestTime);
	// os.writeInt(_decided.size());
	// for (ConsensusInstance ci : _decided) {
	// ci.write(os);
	// }
	// }

	public String toString() {
		return "CatchUpResponse"
				+ (_haveSnapshotOnly ? " - only snapshot available" : "")
				+ " (" + super.toString() + ") for instances: "
				+ _decided.toString() + (_isLastPart ? " END" : "");
	}
}
