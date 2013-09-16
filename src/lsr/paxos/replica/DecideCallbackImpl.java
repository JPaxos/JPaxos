package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.Arrays;
import java.util.Deque;
import java.util.NavigableMap;
import java.util.TreeMap;

import lsr.common.ClientRequest;
import lsr.common.MovingAverage;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.AugmentedBatch;
import lsr.paxos.UnBatcher;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecideCallbackImpl implements DecideCallback {

    private final Replica replica;

    private final SingleThreadDispatcher replicaDispatcher;

    /**
     * Temporary storage for the instances that finished and are not yet
     * executed.
     * 
     * Warning: multi-thread access
     */
    private final NavigableMap<Integer, ConsensusInstance> decidedWaitingExecution =
            new TreeMap<Integer, ConsensusInstance>();

    /** Next instance that will be executed on the replica. Same as in replica */
    private int executeUB;

    /** Used to predict how much time a single instance takes */
    private MovingAverage averageInstanceExecTime = new MovingAverage(0.4, 0);

    /**
     * If predicted time is larger than this threshold, batcher is given more
     * time to collect requests
     */
    private static final double OVERFLOW_THRESHOLD_MS = 250;

    public DecideCallbackImpl(Replica replica, int executeUB) {
        this.replica = replica;
        this.executeUB = executeUB;
        replicaDispatcher = replica.getReplicaDispatcher();
    }

    @Override
    public void onRequestOrdered(final int instance, final ConsensusInstance ci) {
        synchronized (decidedWaitingExecution) {
            decidedWaitingExecution.put(instance, ci);
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

        logger.trace("Executing requests...");

        while (true) {
            ConsensusInstance ci;
            synchronized (decidedWaitingExecution) {
                ci = decidedWaitingExecution.get(executeUB);
            }
            if (ci == null) {
                logger.debug("Cannot continue execution. Next instance not decided: {}", executeUB);
                return;
            }

            AugmentedBatch augmentedBatch = null;

            if (processDescriptor.indirectConsensus) {
                logger.info("Executing instance: {}", executeUB);
                Deque<ClientBatchID> batch = ci.getClientBatchIds();
                if (batch.size() == 1 && batch.getFirst().isNop()) {
                    replica.executeNopInstance(executeUB);
                } else {
                    long start = System.currentTimeMillis();
                    for (ClientBatchID bId : batch) {
                        assert !bId.isNop();

                        ClientRequest[] requests = ClientBatchStore.instance.getBatch(bId);
                        logger.debug("Executing batch: {}", bId);
                        replica.executeClientBatchAndWait(executeUB, requests);
                    }
                    averageInstanceExecTime.add(System.currentTimeMillis() - start);
                }
            } else {
                ClientRequest[] requests;
                if (processDescriptor.augmentedPaxos) {
                    augmentedBatch = new AugmentedBatch(ci.getValue());
                    requests = augmentedBatch.getRequests();
                } else {
                    requests = UnBatcher.unpackCR(ci.getValue());
                }
                if (logger.isDebugEnabled(processDescriptor.logMark_OldBenchmark)) {
                    logger.info(processDescriptor.logMark_OldBenchmark, "Executing instance: {} {}",
                            executeUB, Arrays.toString(requests));
                } else {
                    logger.info("Executing instance: {}", executeUB);
                }
                long start = System.currentTimeMillis();
                replica.executeClientBatchAndWait(executeUB, requests);
                averageInstanceExecTime.add(System.currentTimeMillis() - start);
            }

            // Done with all the client batches in this instance
            replica.instanceExecuted(executeUB, augmentedBatch);
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

    @Override
    public boolean hasDecidedNotExecutedOverflow() {
        double predictedTime = averageInstanceExecTime.get() * decidedWaitingExecution.size();
        return predictedTime >= OVERFLOW_THRESHOLD_MS;
    }

    static final Logger logger = LoggerFactory.getLogger(DecideCallbackImpl.class);
}
