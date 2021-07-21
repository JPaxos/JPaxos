package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lsr.paxos.replica.ClientBatchID;

/**
 * Once a replica is missing some batch values, it sends this message to a
 * replica that has the value.
 */
public class AskForClientBatch extends Message {
    private static final long serialVersionUID = 1L;
    private List<ClientBatchID> neededBatches;

    public AskForClientBatch(List<ClientBatchID> neededBatches) {
        super(-1);
        assert neededBatches.size() > 0;
        this.neededBatches = neededBatches;
    }

    public AskForClientBatch(ClientBatchID neededBatch) {
        this(Collections.singletonList(neededBatch));
    }

    public AskForClientBatch(DataInputStream input) throws IOException {
        super(input);
        int count = input.readInt();
        neededBatches = new ArrayList<ClientBatchID>(count);
        for (int i = 0; i < count; ++i) {
            neededBatches.add(new ClientBatchID(input));
        }
    }

    public AskForClientBatch(ByteBuffer bb) {
        super(bb);
        int count = bb.getInt();
        neededBatches = new ArrayList<ClientBatchID>(count);
        for (int i = 0; i < count; ++i) {
            neededBatches.add(new ClientBatchID(bb));
        }
    }

    public MessageType getType() {
        return MessageType.AskForClientBatch;
    }

    public List<ClientBatchID> getNeededBatches() {
        return neededBatches;
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(neededBatches.size());
        for (ClientBatchID cbid : neededBatches) {
            cbid.writeTo(bb);
        }
    }

    public int byteSize() {
        return super.byteSize() + 4 + (ClientBatchID.byteSizeS() * neededBatches.size());
    }

    public String toString() {
        return AskForClientBatch.class.getSimpleName() + " for " + neededBatches;
    }
}
