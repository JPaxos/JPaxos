package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Accept extends Message {
	private static final long serialVersionUID = 1L;
	// private final byte[] _value;
	private final int _instanceId;

	// public Accept(int view, int instanceId, byte[] value) {
	// super(view);
	// _instanceId = instanceId;
	// _value = value;
	// }

	// public Accept(Propose message) {
	// super(message.getView());
	// _value = message.getValue();
	// _instanceId = message.getInstanceId();
	// }

	public Accept(Propose message) {
		super(message.getView());
		_instanceId = message.getInstanceId();
	}

	public Accept(int view, int instanceId) {
		super(view);
		_instanceId = instanceId;
	}

	// public Accept(DataInputStream input) throws IOException {
	// super(input);
	// _instanceId = input.readInt();
	// _value = new byte[input.readInt()];
	// input.readFully(_value);
	// }
	//
	// public int getInstanceId() {
	// return _instanceId;
	// }
	//
	// public byte[] getValue() {
	// return _value;
	// }
	//
	// @Override
	// public MessageType getType() {
	// return MessageType.Accept;
	// }
	//
	//
	//
	// public int hashCode() {
	// final int prime = 31;
	// int result = 1;
	// result = prime * result + _instanceId;
	// result = prime * result + Arrays.hashCode(_value);
	// return result;
	// }
	//
	// public boolean equals(Object obj) {
	// if (this == obj)
	// return true;
	// if (obj == null)
	// return false;
	// if (getClass() != obj.getClass())
	// return false;
	// Accept other = (Accept) obj;
	// if (_instanceId != other._instanceId)
	// return false;
	// if (!Arrays.equals(_value, other._value))
	// return false;
	// return true;
	// }
	//
	// @Override
	// public String toString() {
	// return "Accept(" + super.toString() + ", instance:" + getInstanceId() +
	// ")";
	// }
	//
	// @Override
	// protected void write(ByteBuffer bb) throws IOException {
	// bb.putInt(_instanceId);
	// bb.putInt(_value.length);
	// bb.put(_value);
	// }
	// // protected void write(DataOutputStream os) throws IOException {
	// // os.writeInt(_instanceId);
	// // os.writeInt(_value.length);
	// // os.write(_value);
	// //}
	//
	// public int byteSize() {
	// return super.byteSize() + 4 + 4 + _value.length;
	// }
	public Accept(DataInputStream input) throws IOException {
		super(input);
		_instanceId = input.readInt();
	}

	public int getInstanceId() {
		return _instanceId;
	}

	@Override
	public MessageType getType() {
		return MessageType.Accept;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _instanceId;
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Accept other = (Accept) obj;
		if (_instanceId != other._instanceId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Accept(" + super.toString() + ", i:" + getInstanceId() + ")";
	}

	@Override
	protected void write(ByteBuffer bb) throws IOException {
		bb.putInt(_instanceId);
	}

	public int byteSize() {
		return super.byteSize() + 4;
	}

}
