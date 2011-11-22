package lsr.paxos.replica;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientRequest;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.ForwardClientRequest;
import lsr.paxos.network.Network;
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
    private final Network network;

    private final int localId;

    private final ClientBatchStore repRequests;

    public ClientRequestBatcher(Network network, ClientBatchStore repRequests) {
        this.network = network;
        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        this.localId = pd.localId;
        this.repRequests = repRequests;
        this.forwardMaxBatchDelay = pd.config.getIntProperty(FORWARD_MAX_BATCH_DELAY, DEFAULT_FORWARD_MAX_BATCH_DELAY);
        this.forwardMaxBatchSize = pd.config.getIntProperty(FORWARD_MAX_BATCH_SIZE, DEFAULT_FORWARD_MAX_BATCH_SIZE);
        logger.config(FORWARD_MAX_BATCH_DELAY + "=" + forwardMaxBatchDelay);
        logger.config(FORWARD_MAX_BATCH_SIZE + "=" + forwardMaxBatchSize);
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
                //                    logger.fine("Timeout expired.");
                // Timeout expired
                sendBatch();
            } else {
//                if (logger.isLoggable(Level.FINE)) {
//                    logger.fine("Request: " + request);
//                }
                // There is a new request to forward                    
                if (sizeInBytes == 0){
                    // logger.fine("New batch.");
                    // Batch is empty. Add the new request unconditionally
                    batch.add(request);
                    sizeInBytes += request.byteSize();
                    batchStart = System.currentTimeMillis();
                    // A single request might exceed the maximum size. 
                    // If so, send the batch
                    if (sizeInBytes > forwardMaxBatchSize) {
                        sendBatch();
                    }
                } else {
                    //                        logger.fine("Current batch size: " + sizeInBytes);
                    // Batch is not empty. 
                    if (sizeInBytes + request.byteSize() > forwardMaxBatchSize) {
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

        ReplicaRequestID rid = new ReplicaRequestID(localId, sequencer.getAndIncrement());
        
        // Send to all
        ClientRequest[] batches = new ClientRequest[batch.size()]; 
        batches = batch.toArray(batches);
        ForwardClientRequest fReqMsg = new ForwardClientRequest(rid, batches, repRequests.rcvdUB[localId]);

//        pLogger.logln(rid + " " + fReqMsg.byteSize());
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Forwarding. rid: " + rid + ", size: " + sizeInBytes + ", " + batch);
        }
        network.sendToAll(fReqMsg);
        
        batch.clear();
        sizeInBytes = 0;
    }
    
    static final Logger logger = Logger.getLogger(ClientRequestBatcher.class.getCanonicalName());
}
