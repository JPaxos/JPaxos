package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.common.RequestId;

/**
 * Thread responsible to receive and queue client requests and to prepare batches
 * for proposing.
 * 
 *  The Selector thread calls the {@link #enqueueClientRequest(Request)} method
 *  when it reads a new request. This method places the request in an internal 
 *  queue.
 *  An internal thread, called Batcher, reads from this queue and packs the request 
 *  in batches, respecting the size limits, the maximum batch delay and the number 
 *  of available slots on the send window.
 *  When a new batch is ready, the Batcher thread submits to the Dispatcher thread
 *  a task that will initiate the proposal of the new batch.
 *   
 *  A new batch is created only when there are slot available on the send 
 *  window. This ensures that in case of overload, the requests that cannot 
 *  be served immediately are kept on the internal queue and that the
 *  task queue of the Dispatcher thread is not overloaded with batches that
 *  cannot be proposed right away.
 *  
 *   The internal request queue is also used to throttle the clients, by blocking
 *   the selector thread whenever the request queue is full.
 * 
 * @author Nuno Santos (LSR)
 *
 */

public final class ActiveBatcher implements Runnable {
    /** Stores client requests. Selector thread enqueues requests, Batcher thread dequeues. */
    private final static int MAX_QUEUE_SIZE = 1024;
//    private final BlockingQueue<Request> queue = new LinkedBlockingDeque<Request>(MAX_QUEUE_SIZE);
    private final BlockingQueue<Request> queue = new ArrayBlockingQueue<Request>(MAX_QUEUE_SIZE);
    private Request SENTINEL = new Request(RequestId.NOP, new byte[0]);

    private final int maxBatchSize;
    private final int maxBatchDelay; 
    private final Proposer proposer;
    private Thread batcherThread;

    /** Keeps track of the current number of slots available for starting instances */
    private int windowSize = -1;

    // Increased whenever the batcher is resumed. 
    // Used to discard partly built batches from previous views.
    private volatile int epoch = 0;
    /* Whether the service is suspended (replica not leader) or active (replica is leader) */
    private volatile boolean suspended = true;

    private final Dispatcher dispatcher;
    // protects the queue and the windowSize variable. Use explicit locks as they
    private Object mylock = new Object();

    public ActiveBatcher(Paxos paxos) {
        this.proposer = paxos.getProposer();
        this.dispatcher = paxos.getDispatcher();
        this.maxBatchDelay = ProcessDescriptor.getInstance().maxBatchDelay;
        this.maxBatchSize = ProcessDescriptor.getInstance().batchingLevel;
    }

    public void start() {
        this.batcherThread = new Thread(this, "Batcher");
        batcherThread.start();
    }

    /**
     * Adds a new client request to the list of pending request. 
     * Blocks if queue is full.
     * 
     * Called from the Client manager thread when it 
     * @param request
     * @throws NotLeaderException
     * @throws InterruptedException 
     */
    public boolean enqueueClientRequest(Request request) throws InterruptedException {
        // This block is not atomic, so it may happen that suspended is false when 
        // the test below is done but becomes true before this thread has time to
        // put the request in the queue. So some requests might stay in the queue between
        // view changes and be re-proposed later. The request will be ignored, so it
        // does not violate safety. And it should be rare. Avoiding this possibility
        // would require a lock between suspended and put, which would slow down
        // considerably the good case.
        if (suspended) {
            return false;
        }        
        queue.put(request);
        return true;
    }

    @Override
    public void run() {
        logger.info("ActiveBatcher starting");
        /* Temporary buffer for the requests that will be put in the next batch
         * until the batch is ready to be sent. By delaying serialization of all 
         * proposals until the size of the batch is known, it's possible to create a 
         * byte[] for the batch with the exact size, therefore avoiding the creation 
         * of a temporary buffer.
         */
        ArrayList<Request> batchReqs = new ArrayList<Request>(16);
        try {
            // If a request taken from the queue cannot fit on a batch, save it in this variable
            // for the next batch. BlockingQueue does not have a timed peek and we cannot add the
            // request back to the queue. 
            Request overflowRequest = null;
            // Try to build a batch
            restart:
            while (true) {
                batchReqs.clear();
                // The header takes 4 bytes
                int batchSize = 4;
                int batchEpoch = epoch;

                Request request;
                if (overflowRequest == null) {
                    request = queue.take();
                    if (request == SENTINEL) {
                        // The epoch increased. Abort this batch 
                        assert batchEpoch != epoch : "BatchEpoch: " + batchEpoch + " Epoch " + epoch + ". Should be equal."; 
                        if (logger.isLoggable(Level.FINE)) {                            
                            logger.fine("Epoch increased: batch " + batchEpoch + ", current: " + epoch);
                        }
                        break restart;
                    }
                } else {
                    request = overflowRequest;
                    overflowRequest = null;
                }
                
                batchSize += request.byteSize();
                batchReqs.add(request);
                // Deadline for sending this batch
                long batchDeadline = System.currentTimeMillis() + maxBatchDelay;
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Starting batch.");
                }


                // Fill the batch
                while (true) {
                    long maxWait = batchDeadline - System.currentTimeMillis();
                    // wait for additional requests until either the batch timeout expires 
                    // or the batcher is suspended at least once.
                    request = queue.poll(maxWait, TimeUnit.MILLISECONDS);
                    if (request == null) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Batch timeout");
                        }
                        break;
                    } else if (request == SENTINEL) {
                        assert batchEpoch != epoch : "BatchEpoch: " + batchEpoch + " Epoch " + epoch + ". Should be equal."; 
                        if (logger.isLoggable(Level.FINE)) {                            
                            logger.fine("Epoch increased: batch " + batchEpoch + ", current: " + epoch);
                        }
                        break restart;
                    } else {
                        if (batchSize + request.byteSize() > maxBatchSize) {
                            // Can't include it in the current batch, as it would exceed size limit. 
                            // Save it for the next batch.
                            overflowRequest = request;
                            break;
                        } else {
                            batchSize += request.byteSize();
                            batchReqs.add(request);
                        }
                    }
                }

                // Serialize the batch
                ByteBuffer bb = ByteBuffer.allocate(batchSize);
                bb.putInt(batchReqs.size());
                for (Request req : batchReqs) {
                    req.writeTo(bb);
                }
                final byte[] value = bb.array();
                // Must also pass an array with the request so that the dispatcher thread 
                // has enough information for logging the batch
                final Request[] requests = batchReqs.toArray(
                        new Request[batchReqs.size()]);

                // Wait until there are slots available in the consensus window
                // A semaphore would be perfect, except that we need to reset the count
                // of the semaphore to a given value when the batcher is resumed. This
                // is not possible with the semaphore in Java.
                synchronized (mylock) {
                    while (windowSize <= 0 && batchEpoch == epoch) {
                        mylock.wait();
                    }
                    // Abort batch if epoch increased
                    if (batchEpoch != epoch) { 
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Epoch advanced. From " + batchEpoch + " to " + epoch + ". Discarding batch");
                        }
                        break restart; 
                    }
                    windowSize--;
                }

                dispatcher.dispatch(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            proposer.propose(requests, value);
                        } catch (InterruptedException ex) {
                            // Propagate the interrupt flag to the thread pool running this task
                            logger.warning("Interrupted: " + ex.getMessage());
                            Thread.currentThread().interrupt();
                        }
                    }});
                if (logger.isLoggable(Level.FINE)) {                    
                    logger.fine("Batch dispatched. Number: " + requests.length + ", Window available: " + windowSize + ", queued reqs: " + queue.size());
                }
            }
        } catch (InterruptedException ex) {
            logger.warning("Thread dying: " + ex.getMessage());
        }
    }

    /**  Update the internal window counter. */
    void onInstanceDecided() {
        assert dispatcher.amIInDispatcher();
        synchronized (mylock) {
            windowSize++;
            mylock.notify();
        }
    }

    /** Stops the batcher from creating new batches. Called when the process is demoted */  
    void suspendBatcher() {
        assert dispatcher.amIInDispatcher();
        if (suspended) {
            // Can happen when the leader advances view before finishing preparing.
            // Batcher is started only when the 
            return;
        }
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Suspend batcher. Discarding " + queue.size() + " queued requests.");
        }
        // volatile, ensures that no request are put in the queue after 
        // this line is executed
        suspended = true;
        queue.clear();
        epoch++;
        try {
            queue.put(SENTINEL);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        synchronized (mylock) {
            mylock.notify();
        }
    }

    /** Restarts the batcher, giving it an initial window size. */
    void resumeBatcher(int currentWndSize) {        
        assert dispatcher.amIInDispatcher();
        assert suspended;
        logger.info("Resuming batcher. Wnd size: " + currentWndSize);
        synchronized (mylock) {
            windowSize = currentWndSize;
            mylock.notify();
            suspended = false;
        }
    }

    private final static Logger logger = 
        Logger.getLogger(ActiveBatcher.class.getCanonicalName());
}
