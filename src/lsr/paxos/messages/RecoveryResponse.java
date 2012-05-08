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
public class RecoveryResponse extends Message {

    /**
     * List of all requested instances, which were decided by the sender
     */
    private byte[] message;
	private byte isSnapshot;
	private int catchUpId;
	private int paxosId;

    /** Forwards the time of request, allowing dynamic timeouts for catch-up */
    private long requestTime;

    public RecoveryResponse(int view, int paxosId, byte[] message, boolean isSnapshot, int catchUpId) {
        super(view);
		this.message = message;
		this.paxosId = paxosId;
		this.catchUpId = catchUpId;
		if(isSnapshot) this.isSnapshot = 0;
		else this.isSnapshot = 1;
    }

    public RecoveryResponse(DataInputStream input) throws IOException {
        super(input);
        byte isSnapshot = input.readByte();
		
        int messageSize = input.readInt();
		message = new byte[messageSize];
        for (int i = 0; i < messageSize; i++) {
            message[i] = input.readByte();
        }
		
		catchUpId = input.readInt();
		paxosId = input.readInt();
	}

    public MessageType getType() {
        return MessageType.RecoveryResponse;
    }
	
	public boolean isSnapshot() {
		if(this.isSnapshot == 0) return true;
		else return false;
    }
	
	public byte[] getData() {
        return message;
    }
	
	public int getPaxosId() {
        return paxosId;
    }
	
	public int getCatchUpId() {
        return catchUpId;
    }

    public int byteSize() {
        int sz = super.byteSize() + 1 + 4 + 4 + 4 + message.length;
        return sz;
    }

    public String toString() {
        return "RecoveryResponse" + " (" + super.toString() + ") ( catchUpId: " + catchUpId + ") ( message: " + message + ") isSnapshot: " + isSnapshot;
    }

    protected void write(ByteBuffer bb) {
		bb.put(isSnapshot);
		bb.putInt(message.length);
		for (int i = 0; i < message.length; i++) {
			bb.put(message[i]);
        } 
		bb.putInt(catchUpId);
		bb.putInt(paxosId);
    }
}
