package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.ClientRequest;
import lsr.common.NewSingleThreadDispatcher;
import lsr.common.RequestId;
import lsr.common.RequestType;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.Proposer.OnLeaderElectionResultTask;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.replica.ClientRequestBatcher;
import lsr.paxos.replica.ClientRequestManager;
import lsr.paxos.replica.DecideCallback;
import lsr.paxos.replica.Replica;

public class NewPassiveBatcher implements Batcher {

    /** see {@link ClientRequestBatcher#PRELONGED_BATCHING_TIME} */
    public static final int PRELONGED_BATCHING_TIME = ClientRequestBatcher.PRELONGED_BATCHING_TIME;
    public static final int BATCH_HEADER_SIZE = 4;
    public static final int ONDEMAND_ACCEPT_THRESHOLD = processDescriptor.batchingLevel / 2;

    // // // local data // // //

    // batch has been requested and not passed on
    private volatile boolean batchRequested = false;
    // batch requested, not passed on and batch delay expired
    private boolean instantBatch = false;
    private ScheduledFuture<?> timeOutTaskF = null;

    private List<ClientRequest> underConstructionBatch = new ArrayList<ClientRequest>();
    private int underConstructionSize = BATCH_HEADER_SIZE;

    private ConcurrentLinkedQueue<byte[]> fullBatches = new ConcurrentLinkedQueue<byte[]>();

    private volatile SingleThreadDispatcher batcherThread = null;

    // requests received while batcher was inactive
    private Map<RequestId, ClientRequest> stashedWhilePreparing = new ConcurrentHashMap<RequestId, ClientRequest>();

    // // // other JPaxos modules // // //

    private final ProposerImpl proposer;
    private final NewSingleThreadDispatcher paxosDispatcher;
    private DecideCallback decideCallback = null;

    private final Replica replica;

    // // // code // // //

    public NewPassiveBatcher(Paxos paxos, Replica replica) {
        this.replica = replica;
        this.proposer = (ProposerImpl) paxos.getProposer();
        this.paxosDispatcher = paxos.getDispatcher();

    }

    public void setDecideCallback(DecideCallback decideCallback) {
        this.decideCallback = decideCallback;
    }

    public void start() {
    }

    @Override
    public void enqueueClientRequest(RequestType requestType, ClientRequestBatcher cBatcher)
            throws InterruptedException {
        assert requestType instanceof ClientRequest : "This batcher suppports only ClietRequest";
        final ClientRequest request = (ClientRequest) requestType;

        //
        // WARNING: called from SELECTOR thread directly!
        //

        SingleThreadDispatcher currBatcherThread = batcherThread;

        if (currBatcherThread == null) {
            switch (proposer.getState()) {
                case PREPARING:
                    // happens while attempting to claim leadership
                    stashedWhilePreparing.put(request.getRequestId(), request);
                    break;
                case PREPARED:
                    // happens in race condition upon gaining leadership
                    /* ~ ~ ~ fall through ~ ~ ~ */
                case INACTIVE:
                    // happens in race condition upon loosing leadership
                    cBatcher.enqueueRequest(request);
                    break;
                default:
                    // should never happen
                    throw new RuntimeException("Proposer in unknown state " + proposer.getState());
            }
            return;
        }

        currBatcherThread.execute(() -> enqueueClientRequestInternal(request));
    }

    private void enqueueClientRequestInternal(ClientRequest request) {

        if (!underConstructionBatch.isEmpty()) {
            if (request.byteSize() + underConstructionSize > processDescriptor.batchingLevel)
                // request doesn't fit anymore
                finishBatch();
        }
        underConstructionBatch.add(request);
        underConstructionSize += request.byteSize();

        if (instantBatch || underConstructionSize > processDescriptor.batchingLevel)
            // single request is bigger than batching lvl
            finishBatch();
    }

    private void finishBatch() {
        // The assert below is commented out since it can run into a race
        // condition with suspendBatcher which is called from paxos thread
        // assert batcherThread.amIInDispatcher();

        assert underConstructionSize > BATCH_HEADER_SIZE;
        byte[] newBatch = new byte[underConstructionSize];
        ByteBuffer bb = ByteBuffer.wrap(newBatch);
        bb.putInt(underConstructionBatch.size());
        for (ClientRequest req : underConstructionBatch) {
            req.writeTo(bb);
        }
        assert (bb.remaining() == 0);
        fullBatches.add(newBatch);
        logger.debug("Prepared batch with {} requests of size {}; instant is {}",
                underConstructionBatch.size(), underConstructionSize, instantBatch);
        underConstructionBatch.clear();
        underConstructionSize = BATCH_HEADER_SIZE;
        if (batchRequested) {
            if (timeOutTaskF != null) {
                timeOutTaskF.cancel(false);
                timeOutTaskF = null;
            }
            batchRequested = false;
            instantBatch = false;
            proposer.notifyAboutNewBatch();
        }
    }

    @Override
    public byte[] requestBatch() {
        byte[] batch = fullBatches.poll();
        if (batch == null) {
            SingleThreadDispatcher currBatcherThread = batcherThread;
            if (currBatcherThread == null)
                return null;
            currBatcherThread.executeAndWait(() -> requestBatchInternal());
            batch = fullBatches.poll();
        }
        return batch;
    }

    protected void requestBatchInternal() {
        SingleThreadDispatcher currBatcherThread = batcherThread;
        if (currBatcherThread != null && !batchRequested) {
            batchRequested = true;
            assert timeOutTaskF == null;

            if (processDescriptor.maxBatchDelay == 0)
                currBatcherThread.executeAndWait(() -> timedOut());
            else
                timeOutTaskF = currBatcherThread.schedule(() -> timedOut(),
                        processDescriptor.maxBatchDelay, TimeUnit.MILLISECONDS);
        }
    }

    protected void timedOut() {
        SingleThreadDispatcher currentBatcherThread = batcherThread;
        if (currentBatcherThread == null)
            return;
        if (decideCallback.hasDecidedNotExecutedOverflow()) {
            // gtfo JPaxos, you decided too much. execute it first.
            logger.debug("Delaying batcher timeout - decided and not executed overflow");
            timeOutTaskF = currentBatcherThread.schedule(() -> timedOut(),
                    PRELONGED_BATCHING_TIME, TimeUnit.MILLISECONDS);
            return;
        }
        logger.debug("Batcher timeout expired.");
        timeOutTaskF = null;
        if (!underConstructionBatch.isEmpty()) {
            finishBatch();
        } else {
            assert (batchRequested);
            instantBatch = true;
        }
    }

    @Override
    public void suspendBatcher() {
        assert paxosDispatcher.amIInDispatcher();
        final SingleThreadDispatcher oldBatcherThread = batcherThread;
        if (oldBatcherThread == null)
            return;
        batcherThread = null;
        oldBatcherThread.executeAndWait(new Runnable() {
            @Override
            public void run() {
                oldBatcherThread.shutdownNow();
                logger.info("Suspend batcher");
            }
        });
    }

    @Override
    public void resumeBatcher(int nextInstanceId) {
        assert paxosDispatcher.amIInDispatcher();
        assert batcherThread == null;
        logger.info("Resuming batcher.");
        batcherThread = new SingleThreadDispatcher("Batcher");
        batcherThread.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                // This batcher is respawned now and then due to Java
                // miserableness in scheduling, and minimum-synchronization
                // thread architecture makes it possible to schedule something
                // past shutdown. If so, warn only.
                if (executor.isShutdown()) {
                    logger.debug("Batcher task scheduled post shutdown: {}", r);
                    return;
                }
                throw new RuntimeException("" + r + " " + executor);
            }
        });
        batcherThread.start();
    }

    @Override
    public void instanceExecuted(int instanceId, ClientRequest[] requests) {
    }

    /* This task handles client requests accumulated while preparing */
    @Override
    public OnLeaderElectionResultTask preparingNewView() {
        return new OnLeaderElectionResultTask() {
            @Override
            public void onPrepared() {
                assert batcherThread != null;
                final Map<RequestId, ClientRequest> oldStash = stashedWhilePreparing;
                stashedWhilePreparing = new ConcurrentHashMap<RequestId, ClientRequest>();
                logger.debug(
                        "Batcher is about to un-stash {} client requests gathered while preparing",
                        oldStash.size());
                batcherThread.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (ClientRequest request : oldStash.values()) {
                            enqueueClientRequestInternal(request);
                        }
                    }
                });
            }

            @Override
            public void onFailedToPrepare() {
                final Map<RequestId, ClientRequest> oldStash = stashedWhilePreparing;
                stashedWhilePreparing = new ConcurrentHashMap<RequestId, ClientRequest>();
                logger.debug(
                        "Batcher is about to forward to the new leader {} client requests gathered while (unsuccessfull) preparing",
                        oldStash.size());
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        ClientRequestManager clientRequestManager = replica.getRequestManager();
                        for (ClientRequest request : oldStash.values()) {
                            try {
                                clientRequestManager.onClientRequest(request, null);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }).start();
            }
        };
    }

    private final static Logger logger = LoggerFactory.getLogger(NewPassiveBatcher.class);
}
