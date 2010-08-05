package lsr.leader.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class Ping extends Message {
	private static final long serialVersionUID = 1L;

	public Ping(int view) {
		super(view);
	}

	public Ping(int view, long sentTime) {
		super(view, sentTime);
	}

	public Ping(DataInputStream input) throws IOException {
		super(input);
	}
	
	public String toString() {
		return "PING (" + super.toString() + ")";
	}

	@Override
	public MessageType getType() {
		return MessageType.Ping;
	}

	@Override
//	protected void write(DataOutputStream os) throws IOException {
//	}
	protected void write(ByteBuffer bb) throws IOException {
	}
}
