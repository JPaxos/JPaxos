package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.storage.ConsensusInstance;

public class Propose extends Message {
	private static final long serialVersionUID = 1L;
	private final byte[] value;
	private final int instanceId;

	public Propose(int view, int instanceId, byte[] value) {
		super(view);
		assert value != null;
		this.instanceId = instanceId;
		this.value = value;
	}

	public Propose(ConsensusInstance instance) {
		super(instance.getView());
		instanceId = instance.getId();
		value = instance.getValue();
	}

	public Propose(DataInputStream input) throws IOException {
		super(input);

		instanceId = input.readInt();
		value = new byte[input.readInt()];
		input.readFully(value);
	}

	public int getInstanceId() {
		return instanceId;
	}

	public byte[] getValue() {
		return value;
	}

	public MessageType getType() {
		return MessageType.Propose;
	}

	// protected void write(DataOutputStream os) throws IOException {
	// os.writeInt(_instanceId);
	// os.writeInt(_value.length);
	// os.write(_value);
	// }

	protected void write(ByteBuffer bb) throws IOException {
		bb.putInt(instanceId);
		bb.putInt(value.length);
		bb.put(value);
	}

	public int byteSize() {
		return super.byteSize() + 4 + 4 + value.length;
	}

	public String toString() {
		return "Propose(" + super.toString() + ", i:" + getInstanceId() + ")";
	}
}
