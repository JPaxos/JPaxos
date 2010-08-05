package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Nack extends Message {
	private static final long serialVersionUID = 1L;

	public Nack(int view) {
		super(view);
	}

	public Nack(DataInputStream input) throws IOException {
		super(input);
	}

	public MessageType getType() {
		return MessageType.Nack;
	}

//	protected void write(DataOutputStream os) throws IOException {
//	}
	protected void write(ByteBuffer bb) throws IOException {
	}

	public String toString() {
		return "Nack(" + super.toString() + ")";
	}
}
