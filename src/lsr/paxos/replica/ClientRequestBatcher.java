package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.MovingAverage;

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

    public ClientRequestBatcher(ClientBatchManager batchManager) {
        this.batchManager = batchManager;
        this.batcherThread = new Thread(this, "CliReqBatcher");
    }

    public void start() {
        batcherThread.start();
    }

    public void enqueueRequest(ClientRequest fReqMsg) throws InterruptedException {
        cBatcherQueue.put(fReqMsg);
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
                logger.warning("Thread interrupted. Quitting.");
                return;
            }
            batch.add(request);
            sizeInBytes = request.byteSize();

            while (sizeInBytes < processDescriptor.forwardBatchMaxSize) {

                request = null;

                try {
                    int timeToExpire = (int) (batchStart + processDescriptor.forwardBatchMaxDelay - System.currentTimeMillis());
                    request = cBatcherQueue.poll(timeToExpire, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.warning("Thread interrupted. Quitting.");
                    return;
                }

                if (request == null) {
                    // Timeout expired
                    if (logger.isLoggable(Level.FINER))
                        logger.finer("Batch timed out with " + sizeInBytes + "/" +
                                     processDescriptor.forwardBatchMaxSize);
                    break;
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
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer("Predicting that next request won't fit. Left with " +
                                         (sizeInBytes - processDescriptor.forwardBatchMaxSize) +
                                         "bytes, estimated request size:" +
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

        // The batch id is composed of (replicaId, localSeqNumber)
        final ClientBatchID bid = new ClientBatchID(processDescriptor.localId, nextBatchId++);
        // Transform the ArrayList into an array with the exact size.
        final ClientRequest[] batches = batch.toArray(new ClientRequest[batch.size()]);

        batchManager.dispatchForwardNewBatch(bid, batches);

        batch.clear();
        sizeInBytes = 0;
    }

    static final Logger logger = Logger.getLogger(ClientRequestBatcher.class.getCanonicalName());
}
