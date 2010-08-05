package lsr.leader.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class Start extends Message {
	private static final long serialVersionUID = 1L;

	public Start(int view) {
		super(view);
	}

	public Start(int view, long sentTime) {
		super(view, sentTime);
	}

	public Start(DataInputStream input) throws IOException {
		super(input);
	}

	public String toString() {
		return "START (" + super.toString() + ")";
	}

	@Override
	public MessageType getType() {
		return MessageType.Start;
	}

	@Override
	// protected void write(DataOutputStream os) throws IOException {
	// }
	protected void write(ByteBuffer bb) throws IOException {
	}
}
