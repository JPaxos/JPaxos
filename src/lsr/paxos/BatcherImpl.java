package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Request;

public class BatcherImpl implements Batcher {

	/** Size of all the data prepended to requests */
	static final private int headerSize = 4;

	/** Size up to which the message will be batched */
	private int batchingLevel;

	public BatcherImpl(int batchingLevel) {
		this.batchingLevel = batchingLevel;
	}

	@Override
	public byte[] pack(Deque<Request> source, StringBuilder sb, Logger logger) {
		assert !source.isEmpty() : "cannot pack emptiness";

		Request request = source.remove();
		int count = 1;

		int size = Math.max(batchingLevel, headerSize + request.byteSize());
		ByteBuffer buffer = ByteBuffer.allocate(size);

		// later filled with count of instances
		buffer.position(4);

		request.writeTo(buffer);

		if (logger.isLoggable(Level.FINE)) {
			sb.append(", ids=").append(request.getRequestId().toString());
			sb.append("(").append(request.byteSize()).append(")");
		}

		while (!source.isEmpty()) {
			request = source.getFirst();

			if (buffer.remaining() < request.byteSize())
				break;

			request.writeTo(buffer);
			source.remove(request);
			count++;
			
			if (logger.isLoggable(Level.FINE)) {
				sb.append(",").append(request.getRequestId().toString());
				sb.append("(").append(request.byteSize()).append(")");
			}
		}

		buffer.putInt(0, count);
		buffer.flip();

		byte[] value = new byte[buffer.limit()];
		buffer.get(value);

		if (logger.isLoggable(Level.INFO)) {
			sb.append(", Size:").append(value.length);
			sb.append(", k=").append(count);
		}
		
		return value;
	}

	@Override
	public Deque<Request> unpack(byte[] source) {
		ByteBuffer bb = ByteBuffer.wrap(source);
		int count = bb.getInt();

		Deque<Request> requests = new ArrayDeque<Request>(count);

		for (int i = 0; i < count; ++i) {
			requests.add(Request.create(bb));
		}

		assert bb.remaining() == 0 : "Packing/unpacking error";

		return requests;
	}

}
