package lsr.paxos.test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import lsr.common.Configuration;
import lsr.paxos.replica.Replica;
import lsr.service.AbstractService;

/**
 * Pongs the requests to the client.
 */
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
            fireSnapshotMade(seqNo + 1, new byte[] {1}, value);
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

    public static void main(String[] args) throws IOException, InterruptedException,
            ExecutionException {
        if (args.length > 2) {
            usage();
            System.exit(1);
        }
        int localId = Integer.parseInt(args[0]);
        Configuration conf = new Configuration();

        Replica replica = new Replica(conf, localId, new EchoService());

        replica.start();
        System.in.read();
        System.exit(-1);
    }

    private static void usage() {
        System.out.println("Invalid arguments. Usage:\n"
                           + "   java " + EchoService.class.getCanonicalName() + " <replicaID>");
    }
}
