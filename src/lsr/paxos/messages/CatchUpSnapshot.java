package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.Snapshot;

public class CatchUpSnapshot extends Message {
    private static final long serialVersionUID = 1L;

    /** Forwards the time of request, allowing dynamic timeouts for catch-up */
    private long requestTime;

    private Snapshot snapshot;

    public CatchUpSnapshot(int view, long requestTime, Snapshot snapshot) {
        super(view);
        this.requestTime = requestTime;
        this.snapshot = snapshot;
    }

    public CatchUpSnapshot(DataInputStream input) throws IOException {
        super(input);
        requestTime = input.readLong();
        snapshot = new Snapshot(input);
    }

    public long getRequestTime() {
        return requestTime;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public MessageType getType() {
        return MessageType.CatchUpSnapshot;
    }

    public int byteSize() {
        return super.byteSize() + 8 + snapshot.byteSize();
    }

    public String toString() {
        return "CatchUpSnapshot (" + super.toString() + ") nextInstaceID: " +
               snapshot.getNextInstanceId();
    }

    protected void write(ByteBuffer bb) {
        bb.putLong(requestTime);
        snapshot.writeTo(bb);
    }
}
