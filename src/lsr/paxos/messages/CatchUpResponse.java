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
	
    /**
	 * List of all requested instances, which were decided by the sender
	 */
	private int missingInstances;
	private int catchUpId;
	
    /** Forwards the time of request, allowing dynamic timeouts for catch-up */
    private long requestTime;
	
    public CatchUpResponse(int view, int missingInstances, int catchUpId) {
        super(view);
		this.catchUpId = catchUpId;
        this.missingInstances = missingInstances;
    }
	
    public CatchUpResponse(DataInputStream input) throws IOException {
        super(input);
		missingInstances = input.readInt();
		catchUpId = input.readInt();
    }
	
    public MessageType getType() {
        return MessageType.CatchUpResponse;
    }
	
	public int getCatchUpId() {
        return catchUpId;
    }
	
	public int getMissingInstances() {
        return missingInstances;
    }
	
    public int byteSize() {
        int sz = super.byteSize() + 4 + 4;
        return sz;
    }
	
    public String toString() {
        return "CatchUpResponse" + " (" + super.toString() + ") ( catchUpId: " + catchUpId + ") missing: " + missingInstances;
    }
	
    protected void write(ByteBuffer bb) {
		bb.putInt(missingInstances);
		bb.putInt(catchUpId);
    }
}