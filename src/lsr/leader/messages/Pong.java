package lsr.leader.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class Pong extends Message {
    private static final long serialVersionUID = 1L;

    public Pong(int view) {
        super(view);
    }

    public Pong(int view, long sentTime) {
        super(view, sentTime);
    }

    public Pong(DataInputStream input) throws IOException {
        super(input);
    }

    public String toString() {
        return "PONG (" + super.toString() + ")";
    }

    public MessageType getType() {
        return MessageType.Pong;
    }

    protected void write(ByteBuffer bb) throws IOException {
    }
}
