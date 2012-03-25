package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import lsr.paxos.storage.ConsensusInstance;

/**
 * Represents the catch-up mechanism response message
 */
public class CatchUpResponse extends Message {
    private static final long serialVersionUID = 1L;

    /**
     * List of all requested instances, which were decided by the sender
     */
    private List<ConsensusInstance> decided;

    /** Forwards the time of request, allowing dynamic timeouts for catch-up */
    private long requestTime;
    private boolean haveSnapshotOnly = false;
    private boolean periodicQuery = false;
    private boolean isLastPart = true;

    public CatchUpResponse(int view, long requestTime, List<ConsensusInstance> decided) {
        super(view);
        // Create a copy
        this.decided = new ArrayList<ConsensusInstance>(decided);
        this.requestTime = requestTime;
    }

    public CatchUpResponse(DataInputStream input) throws IOException {
        super(input);
        byte flags = input.readByte();
        periodicQuery = (flags & 1) == 0 ? false : true;
        haveSnapshotOnly = (flags & 2) == 0 ? false : true;
        isLastPart = (flags & 4) == 0 ? false : true;
        requestTime = input.readLong();

        decided = new Vector<ConsensusInstance>();
        for (int i = input.readInt(); i > 0; --i) {
            decided.add(new ConsensusInstance(input));
        }
    }

    public void setDecided(List<ConsensusInstance> decided) {
        this.decided = decided;
    }

    public List<ConsensusInstance> getDecided() {
        return decided;
    }

    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public void setSnapshotOnly(boolean haveSnapshotOnly) {
        this.haveSnapshotOnly = haveSnapshotOnly;
    }

    public boolean isSnapshotOnly() {
        return haveSnapshotOnly;
    }

    public void setPeriodicQuery(boolean periodicQuery) {
        this.periodicQuery = periodicQuery;
    }

    public boolean isPeriodicQuery() {
        return periodicQuery;
    }

    public void setLastPart(boolean isLastPart) {
        this.isLastPart = isLastPart;
    }

    public boolean isLastPart() {
        return isLastPart;
    }

    public MessageType getType() {
        return MessageType.CatchUpResponse;
    }

    public int byteSize() {
        int sz = super.byteSize() + 1 + 8 + 4;
        for (ConsensusInstance ci : decided) {
            sz += ci.byteSize();
        }
        return sz;
    }

    public String toString() {
        return "CatchUpResponse" + (haveSnapshotOnly ? " - only snapshot available" : "") + " (" +
               super.toString() + ") for instances: " + decided.toString() +
               (isLastPart ? " END" : "");
    }

    protected void write(ByteBuffer bb) {
        bb.put((byte) ((periodicQuery ? 1 : 0) + (haveSnapshotOnly ? 2 : 0) + (isLastPart ? 4 : 0)));
        bb.putLong(requestTime);
        bb.putInt(decided.size());
        for (ConsensusInstance ci : decided) {
            ci.write(bb);
        }
    }
}
