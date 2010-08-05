package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import lsr.paxos.storage.ConsensusInstance;

public class PrepareOK extends Message {
	private static final long serialVersionUID = 1L;

	private final ConsensusInstance[] _prepared;

	/**
	 * 
	 * @param view
	 * @param value
	 * @param lastValue
	 */
	public PrepareOK(int view, ConsensusInstance[] prepared) {
		super(view);
		_prepared = prepared;
	}

	public PrepareOK(DataInputStream input) throws IOException {
		super(input);
		_prepared = new ConsensusInstance[input.readInt()];
		for (int i = 0; i < _prepared.length; ++i) {
			_prepared[i] = new ConsensusInstance(input);
		}
	}

	public ConsensusInstance[] getPrepared() {
		return _prepared;
	}

	public MessageType getType() {
		return MessageType.PrepareOK;
	}

//	protected void write(DataOutputStream os) throws IOException {
//		os.writeInt(_prepared.length);
//		for (ConsensusInstance ci : _prepared) {
//			ci.write(os);
//		}
//	}

	protected void write(ByteBuffer bb) throws IOException {
		bb.putInt(_prepared.length);
		for (ConsensusInstance ci : _prepared) {
			ci.write(bb);
		}
	}
	
	@Override
	public int byteSize() {
		int size = super.byteSize() + 4;
		for (ConsensusInstance ci : _prepared) {
			size += ci.byteSize();
		}
		return size;
	}

	public String toString() {
		return "PrepareOK(" + super.toString() + ", values: " + Arrays.toString(getPrepared()) + ")";
	}

}
