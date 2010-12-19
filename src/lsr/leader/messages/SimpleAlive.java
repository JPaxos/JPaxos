package lsr.leader.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class SimpleAlive extends Message {
    private static final long serialVersionUID = 1L;

    public SimpleAlive(int view) {
        super(view);
    }

    public SimpleAlive(DataInputStream input) throws IOException {
        super(input);
    }

    protected void write(ByteBuffer os) throws IOException {
    }

    public String toString() {
        return "ALIVE (" + super.toString() + ")";
    }

    public MessageType getType() {
        return MessageType.SimpleAlive;
    }
}
