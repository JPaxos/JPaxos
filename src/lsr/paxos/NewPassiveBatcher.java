package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lsr.common.ClientRequest;
import lsr.common.RequestType;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.replica.ClientRequestBatcher;
import lsr.paxos.replica.DecideCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private List<RequestType> underConstructionBatch = new ArrayList<RequestType>();
    private int underConstructionSize = BATCH_HEADER_SIZE;

    private ConcurrentLinkedQueue<byte[]> fullBatches = new ConcurrentLinkedQueue<byte[]>();

    private volatile SingleThreadDispatcher batcherThread = null;

    // // // other JPaxos modules // // //

    private final ProposerImpl proposer;
    private final SingleThreadDispatcher paxosDispatcher;
    private DecideCallback decideCallback = null;

    // // // code // // //

    public NewPassiveBatcher(Paxos paxos) {
        this.proposer = (ProposerImpl) paxos.getProposer();
        this.paxosDispatcher = paxos.getDispatcher();

    }

    public void setDecideCallback(DecideCallback decideCallback) {
        this.decideCallback = decideCallback;
    }

    public void start() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.Batcher#enqueueClientRequest(lsr.common.RequestType)
     */
    @Override
    public void enqueueClientRequest(final RequestType request) {
        //
        // WARNING: called from SELECTOR thread disrectly!
        //

        SingleThreadDispatcher currBatcherThread = batcherThread;

        // TODO: do something about lost requests.
        if (currBatcherThread == null) {
            logger.debug("Loosing client request {} due to unprepared batcher", request);
            return;
        }

        currBatcherThread.execute(new Runnable() {

            @Override
            public void run() {
                enqueueClientRequestInternal(request);
            }
        });
    }

    private void enqueueClientRequestInternal(RequestType request) {

        if (!underConstructionBatch.isEmpty()) {
            // request doesn't fit anymore
            if (request.byteSize() + underConstructionSize > processDescriptor.batchingLevel)
                finishBatch();
        }
        underConstructionBatch.add(request);
        underConstructionSize += request.byteSize();

        if (instantBatch || underConstructionSize > processDescriptor.batchingLevel)
            // single request is bigger than batching lvl
            finishBatch();
    }

    private void finishBatch() {
        assert batcherThread.amIInDispatcher();
        assert underConstructionSize > BATCH_HEADER_SIZE;
        byte[] newBatch = new byte[underConstructionSize];
        ByteBuffer bb = ByteBuffer.wrap(newBatch);
        bb.putInt(underConstructionBatch.size());
        for (RequestType req : underConstructionBatch) {
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
    public byte[] requestBatch()
    {
        byte[] batch = fullBatches.poll();
        if (batch == null) {
            SingleThreadDispatcher currBatcherThread = batcherThread;
            if (currBatcherThread == null)
                return null;
            currBatcherThread.executeAndWait(new Runnable() {
                public void run() {
                    requestBatchInternal();
                }
            });
            batch = fullBatches.poll();
        }
        return batch;
    }

    protected void requestBatchInternal() {

        if (!batchRequested) {
            batchRequested = true;
            assert timeOutTaskF == null;
            timeOutTaskF = batcherThread.schedule(new Runnable() {

                public void run() {
                    timedOut();
                }
            }, processDescriptor.maxBatchDelay,
                    TimeUnit.MILLISECONDS);
        }
    }

    protected void timedOut() {
        if (decideCallback.hasDecidedNotExecutedOverflow()) {
            // gtfo JPaxos, you decided too much. execute it first.
            logger.debug("Delaying batcher tmeout - decided and not executed overflow");
            timeOutTaskF = batcherThread.schedule(new Runnable() {

                public void run() {
                    timedOut();
                }
            }, PRELONGED_BATCHING_TIME, TimeUnit.MILLISECONDS);
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
        if (batcherThread == null)
            return;
        final SingleThreadDispatcher oldBatcherThread = batcherThread;
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

    private final static Logger logger = LoggerFactory.getLogger(NewPassiveBatcher.class);
}
