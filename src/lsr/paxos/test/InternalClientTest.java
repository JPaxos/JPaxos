package lsr.paxos.test;

import java.util.Random;
import java.util.concurrent.Semaphore;

import lsr.common.Configuration;
import lsr.paxos.replica.Replica;
import lsr.service.AbstractService;

public class InternalClientTest extends AbstractService {

    final Semaphore requestSem;
    int reqCounter;
    int replCounter;
    final int reqSize;
    private final int localId;
    private Replica replica;
    private int executeSeqNo;

    Thread reqSender = new Thread() {
        public void run() {
            try {
                Random r = new Random();
                while (true) {
                    requestSem.acquire();
                    if (reqCounter-- < 0)
                        break;
                    byte[] req = new byte[reqSize];
                    r.nextBytes(req);
                    req[0] = (byte) localId;
                    System.err.println("abcast  " + requestSem.availablePermits() + "  " +
                                       reqCounter);
                    replica.executeNonFifo(req);
                }
                System.err.println("  FINISHED ABCAST  ");
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };
    };

    {
        reqSender.setDaemon(true);
        reqSender.setName("ReqSender");
    }

    public InternalClientTest(String[] args, int localId) {
        this.localId = localId;
        reqCounter = replCounter = Integer.valueOf(args[1]);
        requestSem = new Semaphore(Integer.valueOf(args[2]));
        reqSize = Integer.valueOf(args[3]);

    }

    @Override
    public byte[] execute(byte[] value, int executeSeqNo) {
        this.executeSeqNo = executeSeqNo;
        if (value[0] == (byte) localId) {
            requestSem.release();

            if (replCounter-- == 0)
                System.err.println("  FINISHED ADELIVER  " + (char) 033 + "[37m");

            System.err.println("  adeliver  " + requestSem.availablePermits() + "  " + replCounter);
        }

        return new byte[0];
    }

    @Override
    public void askForSnapshot(int lastSnapshotNextRequestSeqNo) {
        forceSnapshot(lastSnapshotNextRequestSeqNo);
    }

    @Override
    public void forceSnapshot(int lastSnapshotNextRequestSeqNo) {
        fireSnapshotMade(executeSeqNo + 1, new byte[128 * reqSize], null);
    }

    @Override
    public void updateToSnapshot(int nextRequestSeqNo, byte[] snapshot) {
        this.executeSeqNo = nextRequestSeqNo - 1;
    }

    @Override
    public void recoveryFinished() {
        super.recoveryFinished();
        reqSender.start();
    }

    public static void main(String[] args) throws Exception {
        int localId = Integer.valueOf(args[0]);
        InternalClientTest service = new InternalClientTest(args, localId);
        Replica replica = new Replica(new Configuration(), localId,
                service);
        service.setReplica(replica);

        replica.start();

        System.in.read();
        System.exit(-1);
    }

    public void setReplica(Replica replica) {
        this.replica = replica;
    }

}
