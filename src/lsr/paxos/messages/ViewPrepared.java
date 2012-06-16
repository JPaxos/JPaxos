package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ViewPrepared extends Message {
    private static final long serialVersionUID = 1L;

    public ViewPrepared(DataInputStream input) throws IOException {
        super(input);
    }

    public ViewPrepared(int view) {
        super(view);
    }

    @Override
    public MessageType getType() {
        return MessageType.ViewPrepared;
    }

    @Override
    protected void write(ByteBuffer bb) {
        // Empty, nothing to write
    }

}
