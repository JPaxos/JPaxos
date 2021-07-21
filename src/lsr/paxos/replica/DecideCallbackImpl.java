package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.ClientRequest;
import lsr.common.CrashModel;
import lsr.common.MovingAverage;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.UnBatcher;
import lsr.paxos.NATIVE.PersistentMemory;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;

public class DecideCallbackImpl implements DecideCallback {

    private final Replica replica;

    private final SingleThreadDispatcher replicaDispatcher;

    /**
     * Attempts to prevents scheduling executeRequestsInternal multiple times.
     * 
     * True iff the task for executing requests is scheduled or running.
     */
    private final AtomicBoolean isExecutingRequests = new AtomicBoolean(false);

    /** Used to predict how much time a single instance takes */
    private MovingAverage averageInstanceExecTime = new MovingAverage(0.4, 0);

    /**
     * If predicted time is larger than this threshold, batcher is given more
     * time to collect requests
     */
    private static final double OVERFLOW_THRESHOLD_MS = 250;

    public DecideCallbackImpl(Replica replica) {
        this.replica = replica;
        replicaDispatcher = replica.getReplicaDispatcher();
    }

    @Override
    public void onRequestOrdered(final int instance, final ConsensusInstance ci) {
        replicaDispatcher.execute(() -> {
            replica.getReplicaStorage().addDecidedWaitingExecution(instance, ci);
            logger.trace("Instance " + ci.getId() + " issued, scheduling request execution");
            boolean wasExecutingRequests = isExecutingRequests.getAndSet(true);
            if (!wasExecutingRequests)
                executeRequestsInternal();
        });
    }

    /** Returns how many instances is the service behind the Paxos protocol */
    public int decidedButNotExecutedCount() {
        return replica.getReplicaStorage().decidedWaitingExecutionCount();
    }

    @Override
    public void scheduleExecuteRequests() {
        boolean wasExecutingRequests = isExecutingRequests.getAndSet(true);
        if (!wasExecutingRequests) {
            replicaDispatcher.execute(() -> executeRequestsInternal());
        }
    }

    private void executeRequestsInternal() {
        replicaDispatcher.checkInDispatcher();

        final int executeUB = replica.getReplicaStorage().getExecuteUB();

        ConsensusInstance ci = replica.getReplicaStorage().getDecidedWaitingExecution(executeUB);

        if (ci == null) {
            isExecutingRequests.set(false);
            if (replica.getReplicaStorage().getDecidedWaitingExecution(executeUB) != null) {
                isExecutingRequests.set(true);
                executeRequestsInternal();
            } else
                logger.debug("Cannot continue execution. Next instance not decided: {}", executeUB);
            return;
        }

        ClientRequest[] requests;

        if (processDescriptor.indirectConsensus) {
            logger.info("Executing instance: {}", executeUB);
            Deque<ClientBatchID> batch = ci.getClientBatchIds();
            if (batch.size() == 1 && batch.getFirst().isNop()) {
                requests = new ClientRequest[0];
            } else {
                long start = System.currentTimeMillis();
                ArrayList<ClientRequest> requestsList = new ArrayList<ClientRequest>();
                for (ClientBatchID bId : batch) {
                    assert !bId.isNop();

                    ClientRequest[] requestsFragment = ClientBatchStore.instance.getBatch(bId);
                    logger.debug("Executing batch: {}", bId);
                    replica.executeClientBatchAndWait(executeUB, requestsFragment);
                    requestsList.addAll(Arrays.asList(requestsFragment));
                }
                requests = (ClientRequest[]) requestsList.toArray();
                averageInstanceExecTime.add(System.currentTimeMillis() - start);
            }
        } else {
            requests = UnBatcher.unpackCR(ci.getValue());
            if (logger.isDebugEnabled(processDescriptor.logMark_OldBenchmark)) {
                logger.info(processDescriptor.logMark_OldBenchmark,
                        "Executing instance: {} {}",
                        executeUB, Arrays.toString(requests));
            } else {
                logger.info("Executing instance: {}", executeUB);
            }
            long start = System.currentTimeMillis();
            replica.executeClientBatchAndWait(executeUB, requests);
            averageInstanceExecTime.add(System.currentTimeMillis() - start);
        }

        // Done with all the client batches in this instance
        if (processDescriptor.crashModel == CrashModel.Pmem)
            PersistentMemory.startThreadLocalTx();
        replica.instanceExecuted(executeUB, requests);
        replica.getReplicaStorage().releaseDecidedWaitingExecution(executeUB);
        replica.getReplicaStorage().incrementExecuteUB();
        if (processDescriptor.crashModel == CrashModel.Pmem)
            PersistentMemory.commitThreadLocalTx();

        /* check if next instance is ready to go */
        if (replica.getReplicaStorage().getDecidedWaitingExecution(executeUB + 1) != null) {
            /*
             * do <b>not</b> call directly self or else the replica dispatcher
             * will be unable to process anything else for a too long time
             */
            replicaDispatcher.execute(new Runnable() {
                public void run() {
                    executeRequestsInternal();
                }
            });
            return;
        }

        isExecutingRequests.set(false);

        /* detect race wit protocol thread */
        if (replica.getReplicaStorage().getDecidedWaitingExecution(executeUB + 1) != null)
            scheduleExecuteRequests();
    }

    public void atRestoringStateFromSnapshot(final int nextInstanceId) {
        replicaDispatcher.checkInDispatcher();
        replica.getReplicaStorage().setExecuteUB(nextInstanceId);
        replica.getReplicaStorage().releaseDecidedWaitingExecutionUpTo(nextInstanceId);
    }

    @Override
    public boolean hasDecidedNotExecutedOverflow() {
        double predictedTime = averageInstanceExecTime.get() * decidedButNotExecutedCount();
        return predictedTime >= OVERFLOW_THRESHOLD_MS;
    }

    static final Logger logger = LoggerFactory.getLogger(DecideCallbackImpl.class);
}
