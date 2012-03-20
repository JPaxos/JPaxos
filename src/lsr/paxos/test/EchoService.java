package lsr.paxos.test;

import java.util.Random;
import java.util.logging.Logger;

import lsr.service.AbstractService;

public class EchoService extends AbstractService {
    private byte[] last = new byte[0];
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
            Logger.getLogger(this.getClass().getCanonicalName()).info("Made snapshot");
        }
        last = value;
        return value;
    }
}
