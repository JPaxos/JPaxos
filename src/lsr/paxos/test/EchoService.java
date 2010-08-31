package lsr.paxos.test;

import java.util.Random;
import java.util.logging.Logger;

import lsr.service.AbstractService;

public class EchoService extends AbstractService {
	byte[] last = new byte[0];
	private final Random random;

	public EchoService() {
		super();
		random = new Random(System.currentTimeMillis() + this.hashCode());
	}

	public byte[] execute(byte[] value, int instanceId, int seqNo) {
		if (random.nextBoolean()) {
			assert (last != null);
			fireSnapshotMade(seqNo, last);
			Logger.getLogger(this.getClass().getCanonicalName()).info("Made snapshot");
		}
		last = value;
		return value;
	}

	public void askForSnapshot(int lastSnapshotInstance) {
		// ignore
	}

	public void forceSnapshot(int lastSnapshotInstance) {
		// ignore
	}

	public void updateToSnapshot(int instanceId, byte[] snapshot) {
		// ignore
	}
}
