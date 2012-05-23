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
public class SnapshotQuery extends Message {

    /**
     * The instanceIdArray has ID of undecided instances, finishing with ID from
     * which we have no higher decided
     */

    public SnapshotQuery(int view) {
        super(view);
    }

    public SnapshotQuery(DataInputStream input) throws IOException {
        super(input);
	}

    public MessageType getType() {
        return MessageType.SnapshotQuery;
    }

    public int byteSize() {
        return super.byteSize();
    }

    public String toString() {
        return "SnapshotQuery " +"(" + super.toString() + ")";
    }

    protected void write(ByteBuffer bb) {
    }
}
