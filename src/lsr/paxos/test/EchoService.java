package lsr.paxos.test;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.Configuration;
import lsr.common.KillOnExceptionHandler;
import lsr.paxos.replica.Replica;
import lsr.service.AbstractService;

/**
 * Pongs the requests to the client.
 */
public class EchoService extends AbstractService {
    private byte[] last = new byte[0];
    private final Random random;

    private volatile int lastSeqNo = 0;
    private final static long SAMPLING_MS = 100;

    public EchoService() {
        super();
        random = new Random(System.currentTimeMillis() + this.hashCode());

        // Creates an executor with named thread that runs task printing stats
        new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            boolean startled = false;

            public Thread newThread(Runnable r) {
                if (startled)
                    throw new RuntimeException();
                startled = true;
                Thread thread = new Thread(r, "EchoServiceRPS");
                thread.setDaemon(true);
                thread.setPriority(Thread.MAX_PRIORITY);
                thread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
                return thread;
            }
        }, new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                throw new RuntimeException("" + r + " " + executor);
            }
        }).scheduleAtFixedRate(new Runnable() {
            private int lastSeenSeqNo = lastSeqNo;
            private long lastSeenTime;
            {
                lastSeenTime = System.currentTimeMillis();
                lastSeenTime -= lastSeenTime % SAMPLING_MS;
            }

            public void run() {
                if (processDescriptor == null)
                    // this happens upon startup
                    return;

                // Hrm.... Java and the behavior of 'scheduleAtFixedRate' are
                // far far from what one expects...
                long elapsed;
                while ((elapsed = System.currentTimeMillis() - lastSeenTime) < SAMPLING_MS)
                    try {
                        Thread.sleep(SAMPLING_MS - elapsed);
                    } catch (InterruptedException e) {
                    }

                int seqNo = lastSeqNo;

                if (logger.isInfoEnabled(processDescriptor.logMark_Benchmark))
                    logger.info(processDescriptor.logMark_Benchmark, "RPS: {}",
                            (seqNo - lastSeenSeqNo) * (1000.0 / elapsed));

                if (logger.isWarnEnabled(processDescriptor.logMark_Benchmark2019))
                    logger.warn(processDescriptor.logMark_Benchmark2019, "RPS {}",
                            (seqNo - lastSeenSeqNo) * (1000.0 / elapsed));

                lastSeenSeqNo = seqNo;
                lastSeenTime += elapsed;
                lastSeenTime -= lastSeenTime % SAMPLING_MS;
            }
        }, SAMPLING_MS - (System.currentTimeMillis() % SAMPLING_MS), SAMPLING_MS,
                TimeUnit.MILLISECONDS);
    }

    public byte[] execute(byte[] value, int seqNo) {
        logger.info("<Service> Executed request no. {}", seqNo);
        if (random.nextInt(20000) == 0) {
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

    public void updateToSnapshot(int nextRequestSeqNo, byte[] snapshot) {
        // ignore
    }

    public static void main(String[] args) throws IOException, InterruptedException,
            ExecutionException {
        if (args.length > 2) {
            usage();
            System.exit(1);
        }
        System.err.format(Locale.ROOT, "%.3f UP\n", System.currentTimeMillis()/1000.0);
        logger.warn("UP"); // this logger statement does not work. Investigate in free time.

        int localId = Integer.parseInt(args[0]);
        Configuration conf = new Configuration();

        Replica replica = new Replica(conf, localId, new EchoService());

        replica.start();
        System.in.read();
        System.exit(-1);
    }

    private static void usage() {
        System.out.println("Invalid arguments. Usage:\n" + "   java " +
                           EchoService.class.getCanonicalName() + " <replicaID>");
    }

    private static final Logger logger = LoggerFactory.getLogger(EchoService.class);
}
