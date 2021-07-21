package lsr.paxos.test;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map.Entry;
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
import lsr.service.Service;

public class HashMapService extends AbstractService implements Service {

    class HashableComparableByteArray {
        public HashableComparableByteArray() {}
        public HashableComparableByteArray(byte [] array) {this.array = array;}
        public byte [] array;
        public int hashCode() {
            return Arrays.hashCode(array);
        }
        public boolean equals(Object obj) {
            if(!(obj instanceof HashableComparableByteArray))
                return false;
            return Arrays.equals(array, ((HashableComparableByteArray)obj).array);
        }
    }
    
    HashMap<HashableComparableByteArray, byte[]> kvmap = new HashMap<HashableComparableByteArray, byte[]>();
    int lastExecuteSeqNo = 0;

    @Override
    public byte[] execute(byte[] value, int executeSeqNo) {
        if (value.length < 5)
            throw new IllegalArgumentException("Too short request");

        ByteBuffer bb = ByteBuffer.wrap(value);
        byte reqType = bb.get();

        int keyLen = bb.getInt();

        if (value.length < 5 + keyLen)
            throw new IllegalArgumentException("Key length exceeds available data");

        byte[] key = new byte[keyLen];
        bb.get(key);

        logger.debug("service executing {} ({}) key len {}, total len {}", executeSeqNo, (char) reqType, keyLen, value.length);

        byte[] resp;

        switch (reqType) {
            case 'G':
                if (value.length != 5 + keyLen)
                    throw new IllegalArgumentException("Surplus data past key");
                resp = kvmap.get(new HashableComparableByteArray(key));
                break;
            case 'P':
                byte[] newVal = new byte[bb.remaining()];
                bb.get(newVal);
                resp = kvmap.put(new HashableComparableByteArray(key), newVal);
                break;
            default:
                throw new IllegalArgumentException("Unknown request type: [" + reqType + "]");
        }
        lastExecuteSeqNo = executeSeqNo;
        if (resp == null)
            resp = new byte[0];
        return resp;
    }

    @Override
    public void askForSnapshot(int lastSnapshotNextRequestSeqNo) {
        forceSnapshot(lastSnapshotNextRequestSeqNo);
    }

    @Override
    public void forceSnapshot(int lastSnapshotNextRequestSeqNo) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt(lastExecuteSeqNo);

            // this is for compliance with C version which stores sha512 sum
            // there
            byte[] placeholder = new byte[64];
            Arrays.fill(placeholder, (byte) 0);
            dos.write(placeholder);

            dos.writeInt(kvmap.size());
            for (Entry<HashableComparableByteArray, byte[]> entry : kvmap.entrySet()) {
                dos.writeInt(entry.getKey().array.length);
                dos.write(entry.getKey().array);
                dos.writeInt(entry.getValue().length);
                dos.write(entry.getValue());
            }

            dos.flush();

            fireSnapshotMade(lastExecuteSeqNo, baos.toByteArray(), null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateToSnapshot(int nextRequestSeqNo, byte[] snapshot) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(snapshot));
            lastExecuteSeqNo = dis.readInt();

            dis.skipBytes(64);

            int size = dis.readInt();
            kvmap = new HashMap<HashableComparableByteArray, byte[]>(size);
            for (; size > 0; size--) {
                byte[] key = new byte[dis.readInt()];
                dis.read(key);
                byte[] value = new byte[dis.readInt()];
                dis.read(value);
                kvmap.put(new HashableComparableByteArray(key), value);
            }

            if (dis.available() != 0)
                throw new IllegalArgumentException("data past snapshot");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final static long SAMPLING_MS = 100;

    public HashMapService() {
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
            private int lastSeenSeqNo = lastExecuteSeqNo;
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

                int seqNo = lastExecuteSeqNo;

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

    public static void main(String[] args)
            throws IOException, InterruptedException, ExecutionException {
        if (args.length > 2) {
            usage();
            System.exit(1);
        }
        System.err.format(Locale.ROOT, "%.3f UP\n", System.currentTimeMillis() / 1000.0);
        logger.warn("UP"); // this logger statement does not work. Investigate
                           // in free time.

        int localId = Integer.parseInt(args[0]);
        Configuration conf = new Configuration();

        Replica replica = new Replica(conf, localId, new HashMapService());

        replica.start();
        System.in.read();
        System.exit(-1);
    }

    private static void usage() {
        System.out.println("Invalid arguments. Usage:\n" + "   java " +
                           HashMapService.class.getCanonicalName() + " <replicaID>");
    }

    private static final Logger logger = LoggerFactory.getLogger(HashMapService.class);
}
