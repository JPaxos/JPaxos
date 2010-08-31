package lsr.common;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents the request of user which will be inserted into state machine
 * after deciding it. After executing this request, <code>Reply</code> message
 * is generated.
 * 
 * @see Reply
 */
public class Request implements Serializable {
	private static final long serialVersionUID = 1L;

	private final RequestId _requestId;
	private final byte[] _value;

	// private byte[] _serialized = null;

	/**
	 * Create new <code>Request</code>.
	 * 
	 * @param requestId
	 *            - id of this request
	 * @param value
	 *            - factor added to service
	 */
	public Request(RequestId requestId, byte[] value) {
		_requestId = requestId;
		_value = value;
	}

	/**
	 * @param value
	 * @return
	 * @deprecated Use {@link #create(ByteBuffer)} 
	 */
	public static Request create(byte[] value) {
		if (value.length == 0)
			return new NoOperationRequest();

		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(value);
			DataInputStream dis = new DataInputStream(bis);
			Long clientId = dis.readLong();
			int sequenceId = dis.readInt();
			RequestId requestId = new RequestId(clientId, sequenceId);
			byte[] val = new byte[dis.readInt()];
			bis.read(val);

			return new Request(requestId, val);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Read a request from the given bytebuffer and advances the position on the
	 * buffer
	 */
	public static Request create(ByteBuffer bb) {
		Long clientId = bb.getLong();
		int sequenceId = bb.getInt();
		RequestId requestId = new RequestId(clientId, sequenceId);

		byte[] val = new byte[bb.getInt()];
		bb.get(val);
		return new Request(requestId, val);
	}

	/**
	 * Returns the id of this request.
	 * 
	 * @return id of request
	 */
	public RequestId getRequestId() {
		return _requestId;
	}

	/**
	 * The value which should be added in service.
	 * 
	 * @return factor added to service
	 */
	public byte[] getValue() {
		return _value;
	}

	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;

		Request request = (Request) obj;
		if (_requestId == null)
			return request._requestId == null;

		if (_requestId.equals(request._requestId)) {
			assert Arrays.equals(_value, request._value) : "Critical: identical RequestID, different value";
			return true;
		}
		return false;
	}

	public String toString() {
		return "id=" + _requestId;
	}

	// public byte[] toByteArray() {
	// if (_serialized != null)
	// return _serialized;
	// try {
	// ByteArrayOutputStream baos = new ByteArrayOutputStream();
	// DataOutputStream dos = new DataOutputStream(baos);
	// dos.writeLong(_requestId.getClientId());
	// dos.writeInt(_requestId.getSeqNumber());
	// dos.writeInt(_value.length);
	// dos.write(_value);
	// _serialized = baos.toByteArray();
	// return _serialized;
	// } catch (IOException e) {
	// throw new RuntimeException(e);
	// }
	// }

	public int byteSize() {
		return 8 + 4 + 4 + _value.length;
	}

	/**
	 * @deprecated Use {@link #writeTo(ByteBuffer)}
	 * @return
	 */
	public byte[] toByteArray() {
		// if (_serialized != null)
		// return _serialized;
		ByteBuffer bb = ByteBuffer.allocate(byteSize());
		writeTo(bb);
		// _serialized = bb.array();
		return bb.array();
	}

	public void writeTo(ByteBuffer bb) {
		bb.putLong(_requestId.getClientId());
		bb.putInt(_requestId.getSeqNumber());
		bb.putInt(_value.length);
		bb.put(_value);
	}

	// public void flushSerialized() {
	// _serialized = null;
	// }

	// public byte[] getSerialized() {
	// return _serialized;
	// }

	// public static String getRqIdFromBArray(byte[] b) {
	// try {
	// DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
	// b));
	// Long clientId = dis.readLong();
	// int sequenceId = dis.readInt();
	// return clientId + ":" + sequenceId;
	// } catch (IOException e) {
	// throw new RuntimeException(e);
	// }
	// }

}
