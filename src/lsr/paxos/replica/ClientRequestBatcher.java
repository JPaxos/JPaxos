package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import lsr.common.ClientRequest;
import lsr.common.MovingAverage;
import lsr.paxos.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This thread builds the batches with the requests received from the client and
 * forwards them to the leader. The selectors place the requests in a queue
 * managed owned by this class. The ForwardingThread reads requests from this
 * queue and groups them into batches.
 * 
 * There is some contention between the Selector threads and the Forwarding
 * thread in the shared queue, but it should be acceptable. For 4 selectors, in
 * a 180s run:
 * 
 * <pre>
 *  (# blocked times, blocking time) (#waits, waiting time)
 * Selector-X (67388 3365) (194668  11240) 
 * ForwardingBatcher (95081 3810) (1210222  96749) 
 *  </pre>
 * 
 * @author Nuno Santos (LSR)
 */
public class ClientRequestBatcher implements Runnable {

    /**
     * If the process executes slower than decides, the batcher waits longer in
     * case the execution queue is large.
     * 
     * This time describes how often the queue size will be checked after normal
     * timeout expires, but the queue in decide callback has a lot of unexecuted
     * requests.
     */
    public static final int PRELONGED_BATCHING_TIME = 50;

    private static int nextBatchId = 1;

    /*
     * Selector threads enqueue requests in this queue. The Batcher thread takes
     * requests from here to prepare batches.
     */
    private final ArrayBlockingQueue<ClientRequest> cBatcherQueue = new ArrayBlockingQueue<ClientRequest>(
            128);

    /*
     * Stores the requests that will make the next batch. We use two queues to
     * minimize contention between the Selector threads and the Batcher thread,
     * since they only have to contend for the first queue, which is accessed
     * very briefly by either thread.
     */
    private final ArrayList<ClientRequest> batch = new ArrayList<ClientRequest>(16);
    // Total size of the requests stored in the batch array.
    private int sizeInBytes = 0;

    private final Thread batcherThread;

    private final ClientBatchManager batchManager;

    private final DecideCallback decideCallback;

    private final ClientRequestForwarder requestForwarder;

    public ClientRequestBatcher(ClientBatchManager batchManager, DecideCallback decideCallback) {
        assert processDescriptor.indirectConsensus;
        this.batchManager = batchManager;
        this.requestForwarder = null;
        this.decideCallback = decideCallback;
        this.batcherThread = new Thread(this, "CliReqBatcher");
    }

    public ClientRequestBatcher(ClientRequestForwarder requestForwarder,
                                DecideCallback decideCallback) {
        assert !processDescriptor.indirectConsensus;
        this.batchManager = null;
        this.requestForwarder = requestForwarder;
        this.decideCallback = decideCallback;
        this.batcherThread = new Thread(this, "CliReqBatcher");
    }

    public void start() {
        batcherThread.start();
    }

    public void enqueueRequest(ClientRequest fReqMsg) throws InterruptedException {
        cBatcherQueue.put(fReqMsg);
    }

    protected static int uniqueRunId = -1;

    public static void generateUniqueRunId(Storage storage) {
        int base = 0;
        switch (processDescriptor.crashModel) {
            case FullSS:
                base = (int) storage.getEpoch()[0];
                break;
            case ViewSS:
                base = storage.getView();
                break;
            case EpochSS:
                base = (int) storage.getEpoch()[processDescriptor.localId];
                break;
            case CrashStop:
                break;
            case Pmem:
                base = (int) storage.getRunUniqueId();
                break;
            default:
                throw new RuntimeException("Unknown crash model for ViewEpoch idgen.");
        }
        uniqueRunId = base * processDescriptor.numReplicas + processDescriptor.localId;
    }

    @Override
    public void run() {
        long batchStart = 0;

        MovingAverage averageRequestSize = new MovingAverage(0.2, 0);

        ClientRequest overflow = null;

        while (true) {
            ClientRequest request;

            try {
                if (overflow == null) {
                    request = cBatcherQueue.take();
                    averageRequestSize.add(request.byteSize());
                    batchStart = System.currentTimeMillis();
                } else {
                    request = overflow;
                    overflow = null;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Thread interrupted. Quitting.");
            }
            batch.add(request);
            sizeInBytes = request.byteSize();

            while (sizeInBytes < processDescriptor.forwardBatchMaxSize) {

                request = null;

                try {
                    int timeToExpire = (int) (batchStart + processDescriptor.forwardBatchMaxDelay - System.currentTimeMillis());
                    request = cBatcherQueue.poll(timeToExpire, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread interrupted. Quitting.");
                }

                if (request == null) {
                    // Timeout expired
                    logger.trace("Batch timed out with {}/{}", sizeInBytes,
                            processDescriptor.forwardBatchMaxSize);

                    // if the service has much to do, one can wait for client
                    // requests longer
                    if (decideCallback.hasDecidedNotExecutedOverflow()) {
                        batchStart = System.currentTimeMillis();
                        if (processDescriptor.forwardBatchMaxDelay < PRELONGED_BATCHING_TIME) {
                            batchStart += PRELONGED_BATCHING_TIME -
                                          processDescriptor.forwardBatchMaxDelay;
                        }
                        logger.info("Prelonging batching in ClientRequestBatcher");
                        continue;
                    } else {
                        break;
                    }
                }

                averageRequestSize.add(request.byteSize());

                if (sizeInBytes + request.byteSize() > processDescriptor.forwardBatchMaxSize) {
                    // request won't fit.
                    batchStart = System.currentTimeMillis();
                    overflow = request;
                    break;
                }

                batch.add(request);
                sizeInBytes += request.byteSize();

                if (sizeInBytes + (averageRequestSize.get() / 2) > processDescriptor.forwardBatchMaxSize) {
                    // small chance to fit the next request.
                    if (cBatcherQueue.isEmpty()) {
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                    "Predicting that next request won't fit. Left with {} bytes, estimated request size: {}",
                                    (sizeInBytes - processDescriptor.forwardBatchMaxSize),
                                    averageRequestSize.get());
                        }
                        break;
                    }
                }
            }

            sendBatch();
        }
    }

    private void sendBatch() {
        assert Thread.currentThread() == batcherThread;
        assert sizeInBytes > 0 : "Trying to send an empty batch.";
        assert uniqueRunId != -1;

        final ClientBatchID bid = new ClientBatchID(uniqueRunId, nextBatchId++);
        // Transform the ArrayList into an array with the exact size.
        final ClientRequest[] batches = batch.toArray(new ClientRequest[batch.size()]);

        if (processDescriptor.indirectConsensus)
            batchManager.dispatchForwardNewBatch(bid, batches);
        else
            requestForwarder.forward(batches);

        batch.clear();
        sizeInBytes = 0;
    }

    static final Logger logger = LoggerFactory.getLogger(ClientRequestBatcher.class);
}
