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

    public byte[] execute(byte[] value, int seqNo) {
        Logger.getLogger(this.getClass().getCanonicalName()).info(
                "<Service> Executed request no." + seqNo);
        if (random.nextInt(10) == 0) {
            assert (last != null);
            fireSnapshotMade(seqNo + 1, last, value);
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
