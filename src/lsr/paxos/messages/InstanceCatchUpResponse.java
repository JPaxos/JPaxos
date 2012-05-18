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
public class InstanceCatchUpResponse extends Message {

    /**
     * List of all requested instances, which were decided by the sender
     */
    private List<ConsensusInstance> decided;
	private int missingInstances;
	private int catchUpId;

    /** Forwards the time of request, allowing dynamic timeouts for catch-up */
    private long requestTime;

    public InstanceCatchUpResponse(int view, int missingInstances, int catchUpId) {
        super(view);
		this.catchUpId = catchUpId;
        this.missingInstances = missingInstances;
    }

    public MessageType getType() {
        return MessageType.InstanceCatchUpResponse;
    }

    public int byteSize() {
        int sz = super.byteSize() + 4 + 4;
        for (ConsensusInstance ci : decided) {
            sz += ci.byteSize();
        }
        return sz;
    }

    public String toString() {
        return "CatchUpResponse" + " (" + super.toString() + ") ( catchUpId: " + catchUpId + ") missing: " + missingInstances;
    }

    protected void write(ByteBuffer bb) {
        for (ConsensusInstance ci : decided) {
            ci.write(bb);
        }
		bb.putInt(missingInstances);
		bb.putInt(catchUpId);
    }
}
