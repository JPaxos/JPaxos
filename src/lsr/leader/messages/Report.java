package lsr.leader.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.common.Util;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class Report extends Message {
	private static final long serialVersionUID = 1L;

	private final double[] rtt;

	public Report(int view, double[] localRTT) {
		super(view);
		this.rtt = localRTT;
	}

	public Report(int view, long sentTime, double[] rtt) {
		super(view, sentTime);
		this.rtt = rtt;
	}

	public Report(DataInputStream input) throws IOException {
		super(input);
		int arraySize = input.readInt();
		rtt = new double[arraySize];
		for (int i = 0; i < arraySize; i++) {
			rtt[i] = input.readDouble();
		}
	}

	public double[] getRTT() {
		return rtt;
	}

	public String toString() {
		return "REPORT (" + super.toString() + " rtt: " + Util.toString(rtt)
				+ ")";
	}

	@Override
	public MessageType getType() {
		return MessageType.Report;
	}

	@Override
	// protected void write(DataOutputStream os) throws IOException {
	// os.writeInt(rtt.length);
	// for(int i = 0; i < rtt.length; i++) {
	// os.writeDouble(rtt[i]);
	// }
	// }
	protected void write(ByteBuffer bb) throws IOException {
		bb.putInt(rtt.length);
		for (int i = 0; i < rtt.length; i++) {
			bb.putDouble(rtt[i]);
		}
	}

	public int byteSize() {
		return super.byteSize() + 4 + rtt.length * 8;
	}
}
