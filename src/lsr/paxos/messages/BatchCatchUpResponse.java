package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;

import lsr.common.Pair;
import lsr.common.Range;
import lsr.common.ClientRequest;
import lsr.paxos.replica.ClientBatchID;

/**
 * Represents the catch-up mechanism request message.
 */
public class InstanceCatchUpResponse extends Message {

    /**
     * The instanceIdArray has ID of undecided instances, finishing with ID from
     * which we have no higher decided
     */
    private ClientBatchID bid;
	private ClientRequest[] batch;

    /**
     * Creates new <code>CatchUpQuery</code> message.
     * 
     * @param view - the view number
     * @param instanceIdArray - id of unknown instances
     */
    public InstanceCatchUpResponse(int view, ClientRequest[] batch, ClientBatchID bid) {
        super(view);
		this.batch = batch;
		this.bid = bid;
    }

    /**
     * Creates new <code>CatchUpQuery</code> message from input stream with
     * serialized message.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs
     */
    public InstanceCatchUpResponse(DataInputStream input) throws IOException {
        super(input);
        int replicaID = input.readInt();
        int sn = input.readInt();
		bid = new ClientBatchID(replicaID, sn);
		
		int size = input.readInt();
        batch = new ClientRequest[size];
        for (int i = 0; i < batch.length; i++) {
            batch[i] = ClientRequest.create(input);
        }
	}

    public MessageType getType() {
        return MessageType.InstanceCatchUpResponse;
    }
	
	public ClientBatchID getClientBatchID() {
        return bid;
    }
	
	public ClientRequest[] getBatch() {
        return batch;
    }
	
    public int byteSize() {
		int sz = super.byteSize() + 4 + 4 + 4;
		for (int i = 0; i < batch.length; i++) {
            sz+=batch[i].byteSize();
        }
        return sz;
    }

    public String toString() {
        return "InstanceCatchUpQuery " +"(" + super.toString() + ")" +
			"(ClientBatchID: " + bid + ")" +
			"(ClientRequest: " + batch + ")";
    }

    protected void write(ByteBuffer bb) {
		bb.putInt(bid.getReplicaId());
		bb.putInt(bid.getSn());
		
        bb.putInt(batch.length);
        for (int i = 0; i < batch.length; i++) {
            batch[i].writeTo(bb);
        }
    }
}
