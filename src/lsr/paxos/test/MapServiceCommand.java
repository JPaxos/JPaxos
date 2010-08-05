package lsr.paxos.test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

public class MapServiceCommand implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Long _key;
	private final Long _a;
	private final Long _b;

	public MapServiceCommand(Long key, Long a, Long b) {
		_key = key;
		_a = a;
		_b = b;
	}

	public MapServiceCommand(byte[] value) throws IOException {
		ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(value);
		DataInputStream dataInput = new DataInputStream(byteArrayInput);
		_key = dataInput.readLong();
		_a = dataInput.readLong();
		_b = dataInput.readLong();
	}

	public Long getKey() {
		return _key;
	}

	public Long getA() {
		return _a;
	}

	public Long getB() {
		return _b;
	}

	public String toString() {
		return String.format("[key=%d, a=%d, b=%d]", _key, _a, _b);
	}
}
