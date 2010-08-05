package lsr.common;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Represents the reply message which replica send to client after handling
 * {@link ClientCommand} request.
 * 
 */
public class ClientReply implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Result _result;
	private final byte[] _value;

	/**
	 * The result type of this reply message
	 * 
	 */
	public enum Result {
		OK, NACK, REDIRECT, BUSY;
	};

	/**
	 * Create new client reply.
	 * 
	 * @param result
	 *            - type of reply
	 * @param value
	 *            - value for this reply
	 */
	public ClientReply(Result result, byte[] value) {
		_result = result;
		_value = value;
	}

	/**
	 * Returns the result of this reply.
	 * 
	 * @return result of reply
	 */
	public Result getResult() {
		return _result;
	}

	/**
	 * Returns the value of this reply.
	 * 
	 * @return value of reply
	 */
	public byte[] getValue() {
		return _value;
	}

	public String toString() {
		return _result + " - " + _value;
	}

	public ClientReply(DataInputStream input) throws IOException {
		_result = Result.values()[input.readInt()];
		_value = new byte[input.readInt()];
		input.readFully(_value);

	}

	public byte[] toByteArray() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			write(dos);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return baos.toByteArray();
	}

	public void write(DataOutputStream output) throws IOException {
		output.writeInt(_result.ordinal());
		output.writeInt(_value.length);
		output.write(_value);
	}
}
