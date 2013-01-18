package lsr.paxos.replica;

import java.util.Deque;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.MovingAverage;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.storage.ClientBatchStore;

public class DecideCallbackImpl implements DecideCallback {

    private final Replica replica;

    private final SingleThreadDispatcher replicaDispatcher;

    /**
     * Temporary storage for the instances that finished and are not yet
     * executed.
     * 
     * Warning: multi-thread access
     */
    private final NavigableMap<Integer, Deque<ClientBatchID>> decidedWaitingExecution =
            new TreeMap<Integer, Deque<ClientBatchID>>();

    /** Next instance that will be executed on the replica. Same as in replica */
    private int executeUB;

    /** Used to predict how much time a single instance takes */
    private MovingAverage averageInstanceExecTime = new MovingAverage(0.4, 0);

    /**
     * If predicted time is larger than this threshold, batcher is given more
     * time to collect requests
     */
    private static final double OVERFLOW_THRESHOLD_MS = 500;

    public DecideCallbackImpl(Paxos paxos, Replica replica, int executeUB) {
        this.replica = replica;
        this.executeUB = executeUB;
        replicaDispatcher = replica.getReplicaDispatcher();
    }

    @Override
    public void onRequestOrdered(final int instance, final Deque<ClientBatchID> requests) {
        synchronized (decidedWaitingExecution) {
            decidedWaitingExecution.put(instance, requests);
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Instance: " + instance + ": " + requests.toString());
        }

        replicaDispatcher.submit(new Runnable() {
            @Override
            public void run() {
                executeRequests();
            }
        });
    }

    /** Returns how many instances is the service behind the Paxos protocol */
    public int decidedButNotExecutedCount() {
        synchronized (decidedWaitingExecution) {
            return decidedWaitingExecution.size();
        }
    }

    private void executeRequests() {
        replicaDispatcher.checkInDispatcher();

        if (decidedWaitingExecution.size() > 100) {
            // !!FIXME!! (JK) inform the proposer to inhibit proposing
        }

        logger.fine("Executing requests...");

        while (true) {
            Deque<ClientBatchID> batch;
            synchronized (decidedWaitingExecution) {
                batch = decidedWaitingExecution.get(executeUB);
            }
            if (batch == null) {
                logger.fine("Cannot continue execution. Next instance not decided: " +
                            executeUB);
                return;
            }

            logger.info("Executing instance: " + executeUB);

            if (batch.size() == 1 && batch.getFirst().isNop()) {
                replica.executeNopInstance(executeUB);
            } else {
                long start = System.currentTimeMillis();
                for (ClientBatchID bId : batch) {
                    assert !bId.isNop();

                    ClientRequest[] requests = ClientBatchStore.instance.getBatch(bId);
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Executing batch: " + bId);
                    }
                    replica.executeClientBatchAndWait(executeUB, requests);
                }
                averageInstanceExecTime.add(System.currentTimeMillis() - start);
            }
            // Done with all the client batches in this instance
            replica.instanceExecuted(executeUB);
            synchronized (decidedWaitingExecution) {
                decidedWaitingExecution.remove(executeUB);
            }
            executeUB++;
        }
    }

    public void atRestoringStateFromSnapshot(final int nextInstanceId) {
        executeUB = nextInstanceId;
        replicaDispatcher.checkInDispatcher();
        synchronized (decidedWaitingExecution) {
            if (!decidedWaitingExecution.isEmpty()) {
                if (decidedWaitingExecution.lastKey() < nextInstanceId) {
                    decidedWaitingExecution.clear();
                } else {
                    while (decidedWaitingExecution.firstKey() < nextInstanceId) {
                        decidedWaitingExecution.pollFirstEntry();
                    }
                }
            }
        }
    }

    static final Logger logger = Logger.getLogger(DecideCallbackImpl.class.getCanonicalName());

    @Override
    public boolean hasDecidedNotExecutedOverflow() {
        double predictedTime = averageInstanceExecTime.get() * decidedWaitingExecution.size();
        return predictedTime >= OVERFLOW_THRESHOLD_MS;
    }
}
