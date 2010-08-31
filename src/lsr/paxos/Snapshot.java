package lsr.paxos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import lsr.paxos.storage.ConsensusInstance;

/**
 * Structure - snapshot wrapped with all necessary additional data
 * 
 * 1) Redundancy
 * 
 * Request seqNo cannot be omitted, as we must know when snapshot was made.
 * 
 * Instance id is needed, as we do not know which instance to catch-up from if
 * recovering from snapshot only.
 * 
 * 2) Notice, that
 * 
 * Once snapshot has been made by the service, it will be noticed on Replica
 * only after a full instance - the Replica uses dispatcher to order these
 * requests. That means we may write additionally the border consensus instance.
 * 
 * @author JK
 */
public class Snapshot {

	/**
	 * Default constructor
	 */
	public Snapshot() {
	}

	/**
	 * Reads previously recorded snapshot from input stream
	 * 
	 * @param input
	 * @throws IOException
	 */
	public Snapshot(DataInputStream input) throws IOException {
		requestSeqNo = input.readInt();
		enclosingIntanceId = input.readInt();

		int size = input.readInt();
		lastRequestIdForClient = new HashMap<Long, Integer>();
		for (int i = 0; i < size; i++) {
			lastRequestIdForClient.put(input.readLong(), input.readInt());
		}

		borderInstance = new ConsensusInstance(input);

		size = input.readInt();
		value = new byte[size];
		input.readFully(value);
	}

	/** The real snapshot - data from the Service */
	public byte[] value;

	/** last executed request sequential number */
	public Integer requestSeqNo;

	/** Id of instance with last executed request */
	public Integer enclosingIntanceId;

	/** RequestId of last executed request for each client */
	public Map<Long, Integer> lastRequestIdForClient;

	/** Instance that has been half-executed */
	public ConsensusInstance borderInstance;

	/**
	 * Writes the snapshot at the end of given {@link ByteBuffer}
	 */
	public void appendToByteBuffer(ByteBuffer bb) {
		bb.putInt(requestSeqNo);
		bb.putInt(enclosingIntanceId);

		bb.putInt(lastRequestIdForClient.size());
		for (Entry<Long, Integer> e : lastRequestIdForClient.entrySet()) {
			bb.putLong(e.getKey());
			bb.putInt(e.getValue());
		}

		borderInstance.write(bb);

		bb.putInt(value.length);
		bb.put(value);
	}

	/**
	 * Returns size of this snapshot in bytes
	 */
	public int byteSize() {
		return 4 + 4 + 4 + (lastRequestIdForClient.size() * (8 + 4)) + borderInstance.byteSize() + 4 + value.length;
	}

	@Override
	public String toString() {
		return "Snapshot Req:" + requestSeqNo + " Inst:" + enclosingIntanceId;
	}

	public void writeTo(DataOutputStream snapshotStream) throws IOException {
		snapshotStream.writeInt(requestSeqNo);
		snapshotStream.writeInt(enclosingIntanceId);

		snapshotStream.writeInt(lastRequestIdForClient.size());
		for (Entry<Long, Integer> e : lastRequestIdForClient.entrySet()) {
			snapshotStream.writeLong(e.getKey());
			snapshotStream.writeInt(e.getValue());
		}

		snapshotStream.write(borderInstance.toByteArray());

		snapshotStream.writeInt(value.length);
		snapshotStream.write(value);
	}
}
