package lsr.paxos.replica;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.paxos.statistics.QueueMonitor;

/**
 * This thread builds the batches with the requests received from the client and forwards
 * them to the leader.  The selectors place the requests in a queue managed owned by this
 * class. The ForwardingThread reads requests from this queue and groups them into batches.
 * 
 *  There is some contention between the Selector threads and the Forwarding thread in 
 *  the shared queue, but it should be acceptable. For 4 selectors, in a 180s run:
 * 
 *  <pre>
 *  (# blocked times, blocking time) (#waits, waiting time)
 * Selector-X (67388 3365) (194668  11240) 
 * ForwardingBatcher (95081 3810) (1210222  96749) 
 *  </pre> 
 * 
 * @author Nuno Santos (LSR)
 */
public class ClientRequestBatcher implements Runnable {
    // Generates ids for the batches of requests.
    private final static AtomicInteger sequencer = new AtomicInteger(1);
    
    public final static String FORWARD_MAX_BATCH_SIZE = "replica.ForwardMaxBatchSize";
    // Corresponds to a ethernet frame
    public final static int DEFAULT_FORWARD_MAX_BATCH_SIZE = 1450;
    public final int forwardMaxBatchSize;

    // In milliseconds
    public final static String FORWARD_MAX_BATCH_DELAY = "replica.ForwardMaxBatchDelay";
    public final static int DEFAULT_FORWARD_MAX_BATCH_DELAY = 20;
    public final int forwardMaxBatchDelay;

//    private final PerformanceLogger pLogger;
    /* Selector threads enqueue requests in this queue. The Batcher thread takes requests
     * from here to prepare batches.
     */
    private final ArrayBlockingQueue<ClientRequest> cBatcherQueue = new ArrayBlockingQueue<ClientRequest>(128);

    /* Stores the requests that will make the next batch. We use two queues to minimize 
     * contention between the Selector threads and the Batcher thread, since they only
     * have to contend for the first queue, which is accessed very briefly by either  thread. 
     */
    private final ArrayList<ClientRequest> batch = new ArrayList<ClientRequest>(16);
    // Total size of the requests stored in the batch array.
    private int sizeInBytes = 0;

    private final Thread batcherThread;

    private final int localId;

    private final ClientBatchManager batchManager;

    public ClientRequestBatcher(ClientBatchManager batchManager) {
        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        this.localId = pd.localId;
        this.batchManager = batchManager;
        this.forwardMaxBatchDelay = pd.config.getIntProperty(FORWARD_MAX_BATCH_DELAY, DEFAULT_FORWARD_MAX_BATCH_DELAY);
        this.forwardMaxBatchSize = pd.config.getIntProperty(FORWARD_MAX_BATCH_SIZE, DEFAULT_FORWARD_MAX_BATCH_SIZE);
        logger.warning(FORWARD_MAX_BATCH_DELAY + "=" + forwardMaxBatchDelay);
        logger.warning(FORWARD_MAX_BATCH_SIZE + "=" + forwardMaxBatchSize);
        this.batcherThread = new Thread(this, "ForwardBatcher");        
        QueueMonitor.getInstance().registerQueue("CReqBatcher", cBatcherQueue);
//        pLogger = PerformanceLogger.getLogger("replica-"+ localId +"ClientBatches");
    }

    public void start() {
        batcherThread.start();
    }

    public void enqueueRequest(ClientRequest fReqMsg) throws InterruptedException {
        //            logger.fine("Enqueuing request: " + req);
        cBatcherQueue.put(fReqMsg);
    }

    @Override
    public void run() {
        long batchStart = -1;

        while (true) {
            ClientRequest request;
            try {
                // If there are no requests waiting to be batched, wait forever for the next request.
                // Otherwise, wait for the remaining of the timeout
                int timeToExpire = (sizeInBytes == 0) ? 
                        Integer.MAX_VALUE :
                            (int) (batchStart+forwardMaxBatchDelay - System.currentTimeMillis());
                //                    if (logger.isLoggable(Level.FINE)) {
                //                        logger.fine("Waiting for " + timeToExpire);
                //                    }
                request = cBatcherQueue.poll(timeToExpire, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warning("Thread interrupted. Quitting.");
                return;
            }

            if (request == null) {
                // Timeout expired
                logger.fine("Timeout expired.");                    
                sendBatch();
            } else {
                // There is a new request to forward                    
                if (sizeInBytes == 0){
                    // Batch is empty. Add the new request unconditionally
                    // logger.fine("New batch.");
                    batch.add(request);
                    sizeInBytes += request.byteSize();
                    batchStart = System.currentTimeMillis();
                    // A single request might exceed the maximum size.
                    // If so, send the batch
                    if (sizeInBytes > forwardMaxBatchSize) {
                        logger.fine("Maximum client batch size exceeded.");
                        sendBatch();
                    }
                } else {
                    // Batch is not empty. 
                    // logger.fine("Current batch size: " + sizeInBytes);
                    if (sizeInBytes + request.byteSize() > forwardMaxBatchSize) {
                        logger.fine("Maximum client batch size exceeded.");
                        // Adding this request would exceed the maximum size. 
                        // Send the batch and start a new batch with the current request. 
                        sendBatch();
                        batchStart = System.currentTimeMillis();
                    }
                    batch.add(request);
                    sizeInBytes += request.byteSize();
                }
            }
        }
    }

    private void sendBatch() {
        assert sizeInBytes > 0 : "Trying to send an empty batch.";

        // The batch id is composed of (replicaId, localSeqNumber)
        final ClientBatchID bid = new ClientBatchID(localId, sequencer.getAndIncrement());
        // Transform the ArrayList into an array with the exact size.
        final ClientRequest[] batches = batch.toArray(new ClientRequest[batch.size()]);
        
        // Change threads, because the message piggybacks the ack vector
        batchManager.getDispatcher().submit(new Runnable() {
            @Override
            public void run() {
                batchManager.sendNextBatch(bid, batches);
            }
        });
        
        batch.clear();
        sizeInBytes = 0;
    }
    
    static final Logger logger = Logger.getLogger(ClientRequestBatcher.class.getCanonicalName());
}
