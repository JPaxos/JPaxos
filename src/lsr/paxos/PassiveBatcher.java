package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import lsr.common.ClientRequest;
import lsr.common.MovingAverage;
import lsr.common.RequestType;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientRequestBatcher;
import lsr.paxos.replica.DecideCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class PassiveBatcher implements Runnable, Batcher {

    private final static int MAX_QUEUE_SIZE = 10 * 1024;

    private final BlockingQueue<RequestType> queue = new ArrayBlockingQueue<RequestType>(
            MAX_QUEUE_SIZE);

    private ClientBatchID SENTINEL = ClientBatchID.NOP;

    private static final RequestType WAKE_UP = new RequestType() {
        @Override
        public void writeTo(ByteBuffer bb) {
        }

        @Override
        public int byteSize() {
            return 0;
        }
    };

    private final ProposerImpl proposer;
    private Thread batcherThread;

    /** see {@link ClientRequestBatcher#PRELONGED_BATCHING_TIME} */
    public static final int PRELONGED_BATCHING_TIME = ClientRequestBatcher.PRELONGED_BATCHING_TIME;

    private DecideCallback decideCallback = null;

    /*
     * Whether the service is suspended (replica not leader) or active (replica
     * is leader)
     */
    private volatile boolean suspended = true;

    private final SingleThreadDispatcher paxosDispatcher;

    public PassiveBatcher(Paxos paxos) {
        this.proposer = (ProposerImpl) paxos.getProposer();
        this.paxosDispatcher = paxos.getDispatcher();
    }

    public void start() {
        batcherThread = new Thread(this, "Batcher");
        batcherThread.setDaemon(true);
        batcherThread.start();
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.Batcher#enqueueClientRequest(lsr.common.RequestType)
     */
    @Override
    public void enqueueClientRequest(RequestType request) {
        /*
         * This block is not atomic, so it may happen that suspended is false
         * when the test below is done, but becomes true before this thread has
         * time to put the request in the queue. So some requests might stay in
         * the queue between view changes and be re-proposed later. The request
         * will be ignored, so it does not violate safety. And it should be
         * rare. Avoiding this possibility would require a lock between
         * suspended and put, which would slow down considerably the good case.
         */

        assert !SENTINEL.equals(request) : request + " " + SENTINEL;

        if (suspended) {
            logger.warn("Cannot enqueue proposal. Batcher is suspended.");
        }
        // This queue should never fill up, the RequestManager.pendingRequests
        // queues will enforce flow control. Use add() instead of put() to throw
        // an exception if the queue fills up.
        try {
            queue.put(request);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private final static int MAX_QUEUED_PROPOSALS = 30;

    /**
     * If more than this amount of batches are ready, wait longer for requests
     * for new batches
     */
    private static final int MANY_QUEUED_PROPOSALS = 5;
    private volatile boolean batchRequested = false;
    private volatile boolean batchUnderConstruction = false;
    private ArrayBlockingQueue<byte[]> batches = new ArrayBlockingQueue<byte[]>(
            MAX_QUEUED_PROPOSALS);

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.Batcher#requestBatch()
     */
    @Override
    public byte[] requestBatch()
    {
        byte[] batch = batches.poll();
        if (batch == null) {
            batchRequested = true;
            if (batchUnderConstruction)
                queue.offer(WAKE_UP);
        }
        return batch;
    }

    @Override
    public void run() {
        logger.info("PassiveBatcher starting");
        /*
         * Temporary buffer for the requests that will be put in the next batch
         * until the batch is ready to be sent. By delaying serialization of all
         * proposals until the size of the batch is known, it's possible to
         * create a byte[] for the batch with the exact size, therefore avoiding
         * the creation of a temporary buffer.
         */
        ArrayList<RequestType> batchReqs = new ArrayList<RequestType>(16);

        /*
         * If we have 5 bytes left for requests, and requests average size is
         * 1024 bytes, there is no point in waiting for a next one.
         * 
         * assuming 0 bytes results in the worst case in waiting for a next
         * request or deadline
         */
        MovingAverage averageRequestSize = new MovingAverage(0.2, 0);

        try {
            // If a request taken from the queue cannot fit on a batch, save it
            // in this variable for the next batch. BlockingQueue does not have
            // a blocking peek and we cannot add the request back to the queue.
            RequestType overflowRequest = null;

            // Try to build a batch
            while (true) {
                batchReqs.clear();
                // The header takes 4 bytes
                int batchSize = 4;

                RequestType request;
                if (overflowRequest == null) {
                    // (possibly) wait for a new request
                    request = queue.take();
                    if (WAKE_UP.equals(request)) {
                        continue;
                    }
                    else if (SENTINEL.equals(request)) {
                        // No longer being the leader. Abort this batch
                        logger.debug("Discarding end of epoch marker.");
                        continue;
                    }
                } else {
                    request = overflowRequest;
                    overflowRequest = null;
                }

                averageRequestSize.add(request.byteSize());
                batchSize += request.byteSize();
                batchReqs.add(request);

                boolean batchTimedOut = processDescriptor.maxBatchDelay == 0;

                // Deadline for sending this batch
                long batchDeadline = batchTimedOut ? 0
                        : (System.currentTimeMillis() + processDescriptor.maxBatchDelay);
                // long batchDeadline = System.currentTimeMillis() +
                // processDescriptor.maxBatchDelay;
                logger.debug("Starting batch.");
                batchUnderConstruction = true;

                // Fill the batch
                while (true) {
                    if (batchSize >= processDescriptor.batchingLevel) {
                        // already full, let's break.
                        logger.debug("Batch full");
                        break;
                    }
                    if (batchSize + (averageRequestSize.get() / 2) >= processDescriptor.batchingLevel) {
                        // small chance to fit the next request.
                        if (queue.isEmpty()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Predicting that next request won't fit. Left with {} bytes, estimated request size: {}",
                                        (batchSize - processDescriptor.batchingLevel),
                                        averageRequestSize.get());
                            }
                            break;
                        }
                    }

                    if (batchTimedOut) {
                        if (!batchRequested)
                            request = queue.take();
                        else
                            request = queue.poll();
                    }
                    else {
                        long maxWait = batchDeadline - System.currentTimeMillis();
                        // wait for additional requests until either the batch
                        // timeout expires or the batcher is suspended at least
                        // once.
                        request = queue.poll(maxWait, TimeUnit.MILLISECONDS);
                    }

                    if (request == null) {
                        if (decideCallback != null &&
                            decideCallback.hasDecidedNotExecutedOverflow()
                            || batches.size() > MANY_QUEUED_PROPOSALS) {
                            batchDeadline = System.currentTimeMillis() +
                                            Math.max(processDescriptor.maxBatchDelay,
                                                    PRELONGED_BATCHING_TIME);;
                            logger.debug("Prelonging batching in ActiveBatcher");
                            continue;
                        } else {
                            if (!batchTimedOut) {
                                logger.debug("Batch timeout");
                            }
                            if (batchRequested)
                                break;
                            batchTimedOut = true;
                        }
                    } else if (WAKE_UP.equals(request)) {
                        continue;
                    } else if (SENTINEL.equals(request)) {
                        logger.debug("Discarding end of epoch marker and partial batch.");
                        break;
                    } else {
                        if (batchSize + request.byteSize() > processDescriptor.batchingLevel) {
                            // Can't include it in the current batch, as it
                            // would exceed size limit.
                            // Save it for the next batch.
                            overflowRequest = request;
                            break;
                        } else {
                            averageRequestSize.add(request.byteSize());
                            batchSize += request.byteSize();
                            batchReqs.add(request);
                        }
                    }
                }

                // Lost leadership, drop the batch.
                if (SENTINEL.equals(request)) {
                    continue;
                }

                batchUnderConstruction = false;

                // Serialize the batch
                ByteBuffer bb = ByteBuffer.allocate(batchSize);
                bb.putInt(batchReqs.size());
                for (RequestType req : batchReqs) {
                    req.writeTo(bb);
                }
                byte[] value = bb.array();

                logger.debug("Batch ready. Number of requests: {}, queued reqs: ",
                        batchReqs.size(), queue.size());

                batches.put(value);
                if (batchRequested)
                {
                    batchRequested = false;
                    proposer.notifyAboutNewBatch();
                }
                logger.debug("Batch dispatched.");
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.Batcher#suspendBatcher()
     */
    @Override
    public void suspendBatcher() {
        assert paxosDispatcher.amIInDispatcher();
        if (suspended) {
            // Can happen when the leader advances view before finishing
            // preparing.
            return;
        }

        logger.info("Suspend batcher. Discarding {} queued requests.", queue.size());
        // volatile, but does not ensure that no request are put in the queue
        // after this line is executed; to discard the requests sentinel is used
        suspended = true;
        queue.clear();
        try {
            queue.put(SENTINEL);
        } catch (InterruptedException e) {
            throw new RuntimeException("should-never-happen", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see lsr.paxos.Batcher#resumeBatcher()
     */
    @Override
    public void resumeBatcher(int nextInstanceId) {
        assert paxosDispatcher.amIInDispatcher();
        logger.info("Resuming batcher.");
        suspended = false;
    }

    public void setDecideCallback(DecideCallback decideCallback) {
        this.decideCallback = decideCallback;
    }

    @Override
    public void instanceExecuted(int instanceId, ClientRequest[] requests) {
    }

    private final static Logger logger = LoggerFactory.getLogger(PassiveBatcher.class);
}
