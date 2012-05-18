package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;

import lsr.common.Pair;
import lsr.common.Range;

/**
 * Represents the catch-up mechanism request message.
 */
public class InstanceCatchUpQuery extends Message {

    /**
     * The instanceIdArray has ID of undecided instances, finishing with ID from
     * which we have no higher decided
     */
    private int instanceId;

    /**
     * Creates new <code>CatchUpQuery</code> message.
     * 
     * @param view - the view number
     * @param instanceIdArray - id of unknown instances
     */
    public InstanceCatchUpQuery(int view, int instanceId) {
        super(view);
		this.instanceId = instanceId;
    }

    /**
     * Creates new <code>CatchUpQuery</code> message from input stream with
     * serialized message.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs
     */
    public InstanceCatchUpQuery(DataInputStream input) throws IOException {
        super(input);
        instanceId = input.readInt();
	}

    public MessageType getType() {
        return MessageType.InstanceCatchUpQuery;
    }
	
	public int getInstanceId() {
        return instanceId;
    }

    public int byteSize() {
        return super.byteSize() + 4;
    }

    public String toString() {
        return "InstanceCatchUpQuery " +"(" + super.toString() + ")" +
				"(instanceId: " + instanceId + ")";
    }

    protected void write(ByteBuffer bb) {
		bb.putInt(instanceId);
    }
}
