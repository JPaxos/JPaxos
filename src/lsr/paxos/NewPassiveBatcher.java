package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.RequestType;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.replica.ClientRequestBatcher;
import lsr.paxos.replica.DecideCallback;

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

    private SingleThreadDispatcher batcherThread = null;

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
        batcherThread = new SingleThreadDispatcher("Batcher");
        batcherThread.start();
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.Batcher#enqueueClientRequest(lsr.common.RequestType)
     */
    @Override
    public void enqueueClientRequest(final RequestType request) {
        batcherThread.execute(new Runnable() {

            @Override
            public void run() {
                enqueueClientRequestInternal(request);
            }
        });
    }

    private void enqueueClientRequestInternal(RequestType request) {

        if (!underConstructionBatch.isEmpty()) {
            // request doesn't fit anymore
            if (request.byteSize() + underConstructionSize > processDescriptor.batchingLevel) {
                logger.info("uc:" + underConstructionSize + " rs:" + request.byteSize());
                finishBatch();
            }
        }
        underConstructionBatch.add(request);
        underConstructionSize += request.byteSize();

        if (instantBatch || underConstructionSize > processDescriptor.batchingLevel) {
            logger.info("uc:" + underConstructionSize + " ib:" + instantBatch);
            // single request is bigger than batching lvl
            finishBatch();
        }
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
        logger.info("Prepared batch with " + underConstructionBatch.size() + " requests of size " +
                    underConstructionSize + " of " + processDescriptor.batchingLevel +
                    "; instant: " + instantBatch);
        underConstructionBatch.clear();
        underConstructionSize = BATCH_HEADER_SIZE;
        if (batchRequested) {
            if (timeOutTaskF != null) {
                timeOutTaskF.cancel(false);
                timeOutTaskF = null;
            }
            batchRequested = false;
            instantBatch = false;
            try {
                proposer.notifyAboutNewBatch();
            } catch (InterruptedException e) {
                throw new RuntimeException("\"die.\"");
            }
        }
    }

    @Override
    public byte[] requestBatch()
    {
        byte[] batch = fullBatches.poll();
        if (batch == null) {
            batcherThread.executeAndWait(new Runnable() {
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
            logger.info("DELAYING BATCHER - TOO MANY UNEXECUTED");
            timeOutTaskF = batcherThread.schedule(new Runnable() {

                public void run() {
                    timedOut();
                }
            }, PRELONGED_BATCHING_TIME, TimeUnit.MILLISECONDS);
            return;
        }
        logger.info("Batcher timed out. " + processDescriptor.maxBatchDelay);
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
        batcherThread.executeAndWait(new Runnable() {
            @Override
            public void run() {
                batcherThread.shutdownNow();
                batcherThread = null;
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Suspend batcher");
                }
            }
        });
    }

    @Override
    public void resumeBatcher(int nextInstanceId) {
        assert paxosDispatcher.amIInDispatcher();
        logger.info("Resuming batcher.");
        start();
    }

    @Override
    public void instanceExecuted(int instanceId, AugmentedBatch augmentedBatch) {
    }

    private final static Logger logger =
            Logger.getLogger(NewPassiveBatcher.class.getCanonicalName());
}
