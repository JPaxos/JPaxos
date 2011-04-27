package lsr.paxos;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import lsr.common.Reply;

/**
 * Structure - snapshot wrapped with all necessary additional data
 * 
 * The snapshot instanceId is the instanceId of the next instance to be
 * executed, so that one might also record a snapshot before any instance has
 * been decided (even if this has no real use, and enabling it is easy)
 * 
 * @author JK
 */
public class Snapshot implements Serializable {
    private static final long serialVersionUID = -7961820683501513465L;

    // TODO TZ - improve comments
    // Replica part
    /** Id of next instance to be executed */
    private int nextIntanceId;
    /** The real snapshot - data from the Service */
    private byte[] value;
    /** RequestId of last executed request for each client */
    private Map<Long, Reply> lastReplyForClient;

    // ServiceProxy part
    /** Next request ID to be executed */
    private int nextRequestSeqNo;
    /** First requestSeqNo for the instance nextIntanceId */
    private int startingRequestSeqNo;
    /** Cache for the last instance, the partially executed one */
    private List<Reply> partialResponseCache;

    /**
     * Creates empty snapshot.
     */
    public Snapshot() {
    }

    /**
     * Reads previously recorded snapshot from input stream.
     * 
     * @param input - the input stream with serialized snapshot
     * @throws IOException if I/O error occurs
     */
    public Snapshot(DataInputStream input) throws IOException {

        // instance id
        nextIntanceId = input.readInt();

        // value
        int size = input.readInt();
        value = new byte[size];
        input.readFully(value);

        // executed requests
        size = input.readInt();
        lastReplyForClient = new HashMap<Long, Reply>(size);
        for (int i = 0; i < size; i++) {
            long key = input.readLong();

            int replySize = input.readInt();
            byte[] reply = new byte[replySize];
            input.readFully(reply);

            lastReplyForClient.put(key, new Reply(reply));
        }

        // request sequential number
        nextRequestSeqNo = input.readInt();

        // first request sequential number in next instance
        startingRequestSeqNo = input.readInt();

        // cached replies for the next instance
        size = input.readInt();
        partialResponseCache = new Vector<Reply>(size);
        for (int i = 0; i < size; i++) {
            int replySize = input.readInt();
            byte[] reply = new byte[replySize];
            input.readFully(reply);

            partialResponseCache.add(new Reply(reply));
        }
    }

    /**
     * @return id of next instance to be executed
     */
    public int getNextInstanceId() {
        return nextIntanceId;
    }

    public void setNextInstanceId(int nextInstanceId) {
        this.nextIntanceId = nextInstanceId;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Map<Long, Reply> getLastReplyForClient() {
        return lastReplyForClient;
    }

    public void setLastReplyForClient(Map<Long, Reply> lastReplyForClient) {
        this.lastReplyForClient = lastReplyForClient;
    }

    public int getNextRequestSeqNo() {
        return nextRequestSeqNo;
    }

    public void setNextRequestSeqNo(int nextRequestSeqNo) {
        this.nextRequestSeqNo = nextRequestSeqNo;
    }

    public int getStartingRequestSeqNo() {
        return startingRequestSeqNo;
    }

    public void setStartingRequestSeqNo(int startingRequestSeqNo) {
        this.startingRequestSeqNo = startingRequestSeqNo;
    }

    public List<Reply> getPartialResponseCache() {
        return partialResponseCache;
    }

    public void setPartialResponseCache(List<Reply> partialResponseCache) {
        this.partialResponseCache = partialResponseCache;
    }

    /**
     * Returns size of this snapshot in bytes
     */
    public int byteSize() {
        int size = 4; // next instance ID
        size += 4 + value.length; // value

        size += 4; // last replies
        for (Reply reply : lastReplyForClient.values()) {
            size += 8 + 4 + reply.byteSize();
        }

        size += 4; // nextSeqNo

        size += 4; // startingSeqNo

        size += 4; // cached replies
        for (Reply reply : partialResponseCache) {
            size += 4 + reply.byteSize();
        }

        return size;
    }

    /**
     * Writes the snapshot at the end of given {@link ByteBuffer}
     */
    public void writeTo(ByteBuffer bb) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            writeTo(new DataOutputStream(baos));
            baos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        bb.put(baos.toByteArray());
    }

    /**
     * Writes the snapshot to the given {@link DataOutputStream}
     */
    public void writeTo(DataOutputStream snapshotStream) throws IOException {
        // instance id
        snapshotStream.writeInt(nextIntanceId);

        // value
        snapshotStream.writeInt(value.length);
        snapshotStream.write(value);

        // executed requests
        snapshotStream.writeInt(lastReplyForClient.size());

        for (Entry<Long, Reply> entry : lastReplyForClient.entrySet()) {
            snapshotStream.writeLong(entry.getKey());

            snapshotStream.writeInt(entry.getValue().byteSize());

            snapshotStream.write(entry.getValue().toByteArray());
        }

        // request sequential number
        snapshotStream.writeInt(nextRequestSeqNo);

        // first request sequential number in next instance
        snapshotStream.writeInt(startingRequestSeqNo);

        // cached replies for the next instance
        snapshotStream.writeInt(partialResponseCache.size());
        for (Reply reply : partialResponseCache) {

            snapshotStream.writeInt(reply.byteSize());
            snapshotStream.write(reply.toByteArray());
        }
    }

    /**
     * Compares two snapshot states.
     * 
     * <ul>
     * <li><b>&lt;0</b> object is older than argument
     * <li><b>0</b> object and argument have similar state, can't say for sure
     * <li><b>&gt;0</b> object is newer than argument
     * </ul>
     */
    public int compareTo(Snapshot other) {
        int compareTo = Integer.valueOf(nextIntanceId).compareTo(other.nextIntanceId);
        if (compareTo == 0) {
            compareTo = Integer.valueOf(nextRequestSeqNo).compareTo(other.nextRequestSeqNo);
        }
        return compareTo;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        Snapshot other = (Snapshot) obj;
        return nextIntanceId == other.nextIntanceId && nextRequestSeqNo == other.nextRequestSeqNo;
    }

    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + nextIntanceId;
        hash = 31 * hash + nextRequestSeqNo;
        return hash;
    }

    public String toString() {
        return "Snapshot inst:" + nextIntanceId;
    }
}
