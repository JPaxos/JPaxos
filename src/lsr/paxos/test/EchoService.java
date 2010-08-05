package lsr.paxos.test;

import lsr.service.AbstractService;

public class EchoService extends AbstractService {
	byte[] last;

	public byte[] execute(byte[] value, int instanceId) {
		last = value;
		return value;
	}

	public void instanceExecuted(int instanceId) {
		if (instanceId % 10 == 0) {
			fireSnapshotMade(instanceId, last);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
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
