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

    private final SingleThreadDispatcher unpackerDispatcher = new SingleThreadDispatcher(
            "DecidedInstUnpacker");

    /**
     * Attempts to prevent scheduling unpackRequests multiple times.
     * 
     * True iff the task for executing requests is scheduled or running.
     */
    private final AtomicBoolean isUnpackingRequests = new AtomicBoolean(false);

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
        unpackerDispatcher.start();
    }

    @Override
    public void onRequestOrdered(final int instance, final ConsensusInstance ci) {
        unpackerDispatcher.execute(() -> {
            replica.getReplicaStorage().addDecidedWaitingExecution(instance, ci);
            logger.trace("Instance " + ci.getId() + " issued, scheduling requests unpacking");
            boolean wasUnpackingRequests = isUnpackingRequests.getAndSet(true);
            if (!wasUnpackingRequests)
                unpackRequests();
        });
    }

    /** Returns how many instances is the service behind the Paxos protocol */
    public int decidedButNotExecutedCount() {
        return replica.getReplicaStorage().decidedWaitingExecutionCount();
    }

    @Override
    public void scheduleExecuteRequests() {
        boolean wasUnpackingRequests = isUnpackingRequests.getAndSet(true);
        if (!wasUnpackingRequests) {
            unpackerDispatcher.execute(() -> unpackRequests());
        }
    }

    private void unpackRequests() {
        unpackerDispatcher.checkInDispatcher();

        final int unpackUB = replica.getReplicaStorage().getUnpackUB();

        ConsensusInstance ci = replica.getReplicaStorage().getDecidedWaitingExecution(unpackUB);

        if (ci == null) {
            isUnpackingRequests.set(false);
            if (replica.getReplicaStorage().getDecidedWaitingExecution(unpackUB) != null) {
                isUnpackingRequests.set(true);
                unpackRequests();
            } else
                logger.debug("Cannot continue unpacking. Next instance not unpacked: {}", unpackUB);
            return;
        }

        ClientRequest[] requests;
        if (!processDescriptor.indirectConsensus)
            requests = UnBatcher.unpackCR(ci.getValue());
        else {
            Deque<ClientBatchID> batch = ci.getClientBatchIds();
            if (batch.size() == 1 && batch.getFirst().isNop())
                requests = new ClientRequest[0];
            else {
                ArrayList<ClientRequest> requestsList = new ArrayList<ClientRequest>();
                for (ClientBatchID bId : batch) {
                    assert !bId.isNop();
                    requestsList.addAll(Arrays.asList(ClientBatchStore.instance.getBatch(bId)));
                }
                requests = (ClientRequest[]) requestsList.toArray();
            }
        }

        replicaDispatcher.execute(() -> executeRequests(unpackUB, requests));

        replica.getReplicaStorage().incrementUnpackUB();

        /* check if next instance is ready to go */
        if (replica.getReplicaStorage().getDecidedWaitingExecution(unpackUB + 1) != null) {
            unpackerDispatcher.execute(() -> unpackRequests());
            return;
        }

        isUnpackingRequests.set(false);

        /* detect race with protocol thread */
        if (replica.getReplicaStorage().getDecidedWaitingExecution(unpackUB + 1) != null)
            scheduleExecuteRequests();
    }

    private void executeRequests(final int unpackUB, ClientRequest[] requests) {
        final int executeUB = replica.getReplicaStorage().getExecuteUB();
        if (executeUB != unpackUB) {
            /*-
             * There is a theoretical possibility that after Replica.handleSnapshot submits
             * a runnable that will call handleSnapshotInternal (which sets new unpackUB)
             * the unpacker thread (with the old unpackUB) will submit an unpacked instance
             * to replica thread. This branch covers that case.
             -*/
            logger.warn("Unpack submitted for execution instance {}, while replica expects {}",
                    unpackUB, executeUB);
            return;
        }

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

        // Done with all the client batches in this instance
        if (processDescriptor.crashModel == CrashModel.Pmem)
            PersistentMemory.startThreadLocalTx();
        replica.instanceExecuted(executeUB, requests);
        replica.getReplicaStorage().releaseDecidedWaitingExecution(executeUB);
        replica.getReplicaStorage().incrementExecuteUB();
        if (processDescriptor.crashModel == CrashModel.Pmem)
            PersistentMemory.commitThreadLocalTx();
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
