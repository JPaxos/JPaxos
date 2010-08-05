package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;

import lsr.common.Pair;

/**
 * Represents the catch-up mechanism request message
 */
public class CatchUpQuery extends Message {

	private static final long serialVersionUID = 1L;

	/**
	 * The _instanceIdArray has ID of undecided instances, finishing with ID
	 * from which we have no higher decided
	 */
	private int[] _instanceIdArray;

	private Pair<Integer, Integer>[] _instanceIdRanges;

	private boolean _snapshotRequest = false;

	private boolean _periodicQuery = false;

	/** Constructors */

	public CatchUpQuery(int view, int[] instanceIdArray,
			Pair<Integer, Integer>[] instanceIdRanges) {
		super(view);
		setInstanceIdRangeArray(instanceIdRanges);
		assert instanceIdArray != null;
		_instanceIdArray = instanceIdArray;
	}

	public CatchUpQuery(int view, List<Integer> instanceIdList,
			List<Pair<Integer, Integer>> instanceIdRanges) {
		super(view);
		assert instanceIdList != null;
		setInstanceIdList(instanceIdList);
		setInstanceIdRangeList(instanceIdRanges);
	}

	@SuppressWarnings("unchecked")
	public CatchUpQuery(DataInputStream input) throws IOException {
		super(input);
		byte flags = input.readByte();
		_periodicQuery = (flags & 1) == 0 ? false : true;
		_snapshotRequest = (flags & 2) == 0 ? false : true;

		_instanceIdRanges = new Pair[input.readInt()];
		for (int i = 0; i < _instanceIdRanges.length; ++i) {
			_instanceIdRanges[i] = new Pair<Integer, Integer>(input.readInt(),
					0);
			_instanceIdRanges[i].setValue(input.readInt());
		}

		_instanceIdArray = new int[input.readInt()];
		for (int i = 0; i < _instanceIdArray.length; ++i) {
			_instanceIdArray[i] = input.readInt();
		}
	}

	/** Setters */

	public void setInstanceIdArray(int[] _instanceIdArray) {
		this._instanceIdArray = _instanceIdArray;
	}

	public void setInstanceIdList(List<Integer> instanceIdList) {
		_instanceIdArray = new int[instanceIdList.size()];
		for (int i = 0; i < instanceIdList.size(); ++i) {
			_instanceIdArray[i] = instanceIdList.get(i);
		}
	}

	public void setSnapshotRequest(boolean snapshotRequest) {
		_snapshotRequest = snapshotRequest;
	}

	public void setPeriodicQuery(boolean periodicQuery) {
		_periodicQuery = periodicQuery;
	}

	public void setInstanceIdRangeArray(
			Pair<Integer, Integer>[] _instanceIdRanges) {
		this._instanceIdRanges = _instanceIdRanges;
	}

	@SuppressWarnings("unchecked")
	public void setInstanceIdRangeList(
			List<Pair<Integer, Integer>> instanceIdRanges) {
		_instanceIdRanges = new Pair[instanceIdRanges.size()];
		for (int i = 0; i < instanceIdRanges.size(); ++i) {
			_instanceIdRanges[i] = instanceIdRanges.get(i);
		}
	}

	/** Getters */

	public int[] getInstanceIdArray() {
		return _instanceIdArray;
	}

	public List<Integer> getInstanceIdList() {
		List<Integer> instanceIdList = new Vector<Integer>();
		for (int i = 0; i < _instanceIdArray.length; ++i) {
			instanceIdList.add(_instanceIdArray[i]);
		}
		return instanceIdList;
	}

	public boolean isSnapshotRequest() {
		return _snapshotRequest;
	}

	public boolean isPeriodicQuery() {
		return _periodicQuery;
	}

	public Pair<Integer, Integer>[] getInstanceIdRangeArray() {
		return _instanceIdRanges;
	}

	public List<Pair<Integer, Integer>> getInstanceIdRangeList() {
		List<Pair<Integer, Integer>> instanceIdRanges = new Vector<Pair<Integer, Integer>>();
		for (int i = 0; i < _instanceIdRanges.length; ++i) {
			instanceIdRanges.add(_instanceIdRanges[i]);
		}
		return instanceIdRanges;
	}

	/** Rest */

	public MessageType getType() {
		return MessageType.CatchUpQuery;
	}

	// protected void write(DataOutputStream os) throws IOException {
	// os.writeByte((_periodicQuery ? 1 : 0) + (_snapshotRequest ? 2 : 0));
	//
	// os.writeInt(_instanceIdRanges.length);
	// for (Pair<Integer, Integer> instance : _instanceIdRanges) {
	// os.writeInt(instance.key());
	// os.writeInt(instance.value());
	// }
	//
	// os.writeInt(_instanceIdArray.length);
	// for (int instance : _instanceIdArray) {
	// os.writeInt(instance);
	// }
	// }

	public String toString() {
		return (_periodicQuery ? "periodic " : "")
				+ "CatchUpQuery "
				+ (_snapshotRequest ? "for snapshot " : "")
				+ "("
				+ super.toString()
				+ ")"
				+ (_instanceIdArray != null ? ((" for ranges:" + getInstanceIdRangeList()
						.toString())
						+ " and for instances:" + getInstanceIdList()
						.toString())
						: "");
	}

	@Override
	protected void write(ByteBuffer bb) throws IOException {
		bb.put((byte) ((_periodicQuery ? 1 : 0) + (_snapshotRequest ? 2 : 0)));
		bb.putInt(_instanceIdRanges.length);
		for (Pair<Integer, Integer> instance : _instanceIdRanges) {
			bb.putInt(instance.key());
			bb.putInt(instance.value());
		}

		bb.putInt(_instanceIdArray.length);
		for (int instance : _instanceIdArray) {
			bb.putInt(instance);
		}
	}

	// @Deprecated
	public int byteSize() {
		return super.byteSize() + 1 + 4 + _instanceIdArray.length * 4 + 4
				+ _instanceIdRanges.length * 2 * 4;
	}

}
