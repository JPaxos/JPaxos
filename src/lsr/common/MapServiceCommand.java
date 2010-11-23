package lsr.common;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

public class MapServiceCommand implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Long key;
	private final Long a;
	private final Long b;

	public MapServiceCommand(Long key, Long a, Long b) {
		this.key = key;
		this.a = a;
		this.b = b;
	}

	public MapServiceCommand(byte[] value) throws IOException {
		ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(value);
		DataInputStream dataInput = new DataInputStream(byteArrayInput);
		key = dataInput.readLong();
		a = dataInput.readLong();
		b = dataInput.readLong();
	}

	public Long getKey() {
		return key;
	}

	public Long getA() {
		return a;
	}

	public Long getB() {
		return b;
	}

	public String toString() {
		return String.format("[key=%d, a=%d, b=%d]", key, a, b);
	}
}
