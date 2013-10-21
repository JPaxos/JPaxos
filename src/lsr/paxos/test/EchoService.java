package lsr.paxos.test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import lsr.common.Configuration;
import lsr.common.ProcessDescriptor;
import lsr.paxos.replica.Replica;
import lsr.service.AbstractService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pongs the requests to the client.
 */
public class EchoService extends AbstractService {
    private byte[] last = new byte[0];
    private final Random random;

    private volatile int lastSeqNo = 0;
    private final static long SAMPLING_MS = 100;
    private final static long MIN_SAMPLING_MS = 25;

    public EchoService() {
        super();
        random = new Random(System.currentTimeMillis() + this.hashCode());

        // Creates an executor with named thread that runs task printing stats
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "EchoServiceRPS");
                thread.setDaemon(true);
                thread.setPriority(Thread.MAX_PRIORITY);
                return thread;
            }
        }).scheduleAtFixedRate(new Runnable() {
            private int lastSeenSeqNo = 0;
            private long lastSeenTime = 0;

            public void run() {
                // Hrm.... Java and the behavior of 'scheduleAtFixedRate' are
                // far far from what one expects...
                long time = System.currentTimeMillis();
                if (time - lastSeenTime <= MIN_SAMPLING_MS)
                    return;
                int seqNo = lastSeqNo;
                logger.info(ProcessDescriptor.processDescriptor.logMark_Benchmark, "RPS: {}",
                        (seqNo - lastSeenSeqNo) * (1000.0 / (time - lastSeenTime)));
                lastSeenSeqNo = seqNo;
                lastSeenTime = time;
            }
        }, SAMPLING_MS, SAMPLING_MS, TimeUnit.MILLISECONDS);
    }

    public byte[] execute(byte[] value, int seqNo) {
        logger.info("<Service> Executed request no. {}", seqNo);
        if (random.nextInt(10000) == 0) {
            assert (last != null);
            fireSnapshotMade(seqNo + 1, new byte[] {1}, value);
            logger.info("Made snapshot");
        }
        last = value;

        lastSeqNo = seqNo;

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

    private static final Logger logger = LoggerFactory.getLogger(EchoService.class);
}
