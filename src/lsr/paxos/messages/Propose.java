package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import lsr.paxos.storage.ConsensusInstance;

public class Propose extends Message {
	private static final long serialVersionUID = 1L;
	private final byte[] _value;
	private final int _instanceId;

	public Propose(int view, int instanceId, byte[] value) {
		super(view);
		assert value != null;
		_instanceId = instanceId;
		_value = value;
	}

	public Propose(ConsensusInstance instance) {
		super(instance.getView());
		_instanceId = instance.getId();
		_value = instance.getValue();
	}

	public Propose(DataInputStream input) throws IOException {
		super(input);
		
		_instanceId = input.readInt();
		_value = new byte[input.readInt()];
		_logger.fine("p0");
		input.readFully(_value);
	}

	public int getInstanceId() {
		return _instanceId;
	}

	public byte[] getValue() {
		return _value;
	}

	public MessageType getType() {
		return MessageType.Propose;
	}

//	protected void write(DataOutputStream os) throws IOException {
//		os.writeInt(_instanceId);
//		os.writeInt(_value.length);
//		os.write(_value);
//	}

	protected void write(ByteBuffer bb) throws IOException {
		bb.putInt(_instanceId);
		bb.putInt(_value.length);
		bb.put(_value);
	}
	
	public int byteSize() {
		return super.byteSize() + 4 + 4 + _value.length;
	}

	public String toString() {
		return "Propose(" + super.toString() + ", i:" + getInstanceId() + ")";
	}
	
	private final static Logger _logger = Logger.getLogger(
			Propose.class.getCanonicalName());
}
