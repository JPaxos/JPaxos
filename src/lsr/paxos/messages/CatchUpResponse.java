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
    private static final long serialVersionUID = 1L; // LISA: what is is for?

    /**
     * List of all requested instances, which were decided by the sender
     */
    private List<ConsensusInstance> decided;
	private int missingInstances;

    /** Forwards the time of request, allowing dynamic timeouts for catch-up */
    private long requestTime;

    public CatchUpResponse(int view, int missingInstances) {
        super(view);
        this.missingInstances = missingInstances;
    }

    public CatchUpResponse(DataInputStream input) throws IOException {
        super(input);
        byte flags = input.readByte();

        decided = new Vector<ConsensusInstance>();
        for (int i = input.readInt(); i > 0; --i) {
            decided.add(new ConsensusInstance(input));
        }
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
        return "CatchUpResponse" + " (" + super.toString() + ") missing: " + missingInstances;
    }

    protected void write(ByteBuffer bb) {
        for (ConsensusInstance ci : decided) {
            ci.write(bb);
        }
		bb.putInt(missingInstances);
    }
}
