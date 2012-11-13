package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientBatch;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.replica.ClientBatchID;

/**
 * Thread responsible to receive and queue client requests and to prepare
 * batches for proposing.
 * 
 * The Selector thread calls the {@link #enqueueClientRequest(ClientBatch)}
 * method when it reads a new request. This method places the request in an
 * internal queue. An internal thread, called Batcher, reads from this queue and
 * packs the request in batches, respecting the size limits, the maximum batch
 * delay and the number of available slots on the send window. When a new batch
 * is ready, the Batcher thread submits to the Dispatcher thread a task that
 * will initiate the proposal of the new batch.
 * 
 * A new batch is created only when there are slot available on the send window.
 * This ensures that in case of overload, the requests that cannot be served
 * immediately are kept on the internal queue and that the task queue of the
 * Dispatcher thread is not overloaded with batches that cannot be proposed
 * right away.
 * 
 * The internal request queue is also used to throttle the clients, by blocking
 * the selector thread whenever the request queue is full.
 * 
 * @author Nuno Santos (LSR)
 */

public class ActiveBatcher implements Runnable {
    /*
     * Architecture
     * 
     * Shared queue between Batcher and Protocol thread: pendingProposals.
     * Batcher add batches directly to pendingProposals, blocking if queue full.
     * Size of queue is a configuration parameter, independent of window size.
     * Should be large enough to allow the Batcher thread to work independently
     * from the Protocol thread for long periods, ie, avoid excessive context
     * switching. Protocol thread starts a new batch whenever both conditions
     * are true: - there is a batch available - there is a window slot available
     * First condition changes from - false to true - when a batch is added to
     * an empty pendingProposals queue. - true to false - when all batches are
     * taken from the pendingProposals queue. Second condition changes from -
     * true to false - whenever the Protocol thread reaches the maximum of
     * pending proposals. - false to true - if an instance is decided when the
     * window is full. The Batcher must make the Protocol thread check for
     * batches whenever there is a new batch available.
     * 
     * Proposer.proposeNext() - single point to start new proposals. Called
     * whenever one of the conditions above may have become true. Tries to start
     * as many instances as possible, ie, until either pendingProposals is empty
     * or all the window slots are taken.
     * 
     * proposeNext() is called in the following situations: - Protocol calls
     * nextPropose() whenever it decides an instance. - Batcher adds a request
     * to the queue and, if queue is empty, enqueues a proposal task that will
     * call proposeNext(). If the queue is not empty, then either the window is
     * full or proposeNext() did not execute since the last time the Batcher
     * thread called enqueueRequest. In the first case, proposeNext() will be
     * called when the next consensus is decided and in the second case the
     * propose task enqueued by the Batcher thread is still waiting to be
     * executed.
     */
    /**
     * Stores client requests. Selector thread enqueues requests, Batcher thread
     * dequeues.
     */
    private final static int MAX_QUEUE_SIZE = 2 * 1024;
    // private final BlockingQueue<Request> queue = new
    // LinkedBlockingDeque<Request>(MAX_QUEUE_SIZE);
    private final BlockingQueue<ClientBatch> queue = new ArrayBlockingQueue<ClientBatch>(
            MAX_QUEUE_SIZE);

    private ClientBatch SENTINEL = new ClientBatch(ClientBatchID.NOP);

    private final int maxBatchSize;
    private final int maxBatchDelay;
    private final ProposerImpl proposer;
    private Thread batcherThread;

    /*
     * Whether the service is suspended (replica not leader) or active (replica
     * is leader)
     */
    private volatile boolean suspended = true;

    private final SingleThreadDispatcher dispatcher;

    public ActiveBatcher(Paxos paxos) {
        this.proposer = (ProposerImpl) paxos.getProposer();
        this.dispatcher = paxos.getDispatcher();
        this.maxBatchDelay = ProcessDescriptor.getInstance().maxBatchDelay;
        this.maxBatchSize = ProcessDescriptor.getInstance().batchingLevel;
    }

    public void start() {
        this.batcherThread = new Thread(this, "Batcher");
        batcherThread.start();
    }

    /**
     * Adds a new client request to the list of pending request. Blocks if queue
     * is full.
     * 
     * Called from the Client manager thread when it
     * 
     * @param request
     * @throws NotLeaderException
     * @throws InterruptedException
     */
    public boolean enqueueClientRequest(ClientBatch request) {
        // This block is not atomic, so it may happen that suspended is false
        // when
        // the test below is done but becomes true before this thread has time
        // to
        // put the request in the queue. So some requests might stay in the
        // queue between
        // view changes and be re-proposed later. The request will be ignored,
        // so it
        // does not violate safety. And it should be rare. Avoiding this
        // possibility
        // would require a lock between suspended and put, which would slow down
        // considerably the good case.
        if (suspended) {
            logger.warning("Cannot enqueue proposal. Batcher is suspended.");
            return false;
        }
        // queue.put(request);
        // This queue should never fill up, the RequestManager.pendingRequests
        // queues will enforce flow control.
        // Use add() to throw an exception if the queue fills up.
        queue.add(request);
        return true;
    }

    @Override
    public void run() {
        logger.info("ActiveBatcher starting");
        /*
         * Temporary buffer for the requests that will be put in the next batch
         * until the batch is ready to be sent. By delaying serialization of all
         * proposals until the size of the batch is known, it's possible to
         * create a byte[] for the batch with the exact size, therefore avoiding
         * the creation of a temporary buffer.
         */
        ArrayList<ClientBatch> batchReqs = new ArrayList<ClientBatch>(16);
        try {
            // If a request taken from the queue cannot fit on a batch, save it
            // in this variable
            // for the next batch. BlockingQueue does not have a timed peek and
            // we cannot add the
            // request back to the queue.
            ClientBatch overflowRequest = null;
            // Try to build a batch
            while (true) {
                batchReqs.clear();
                // The header takes 4 bytes
                int batchSize = 4;

                ClientBatch request;
                if (overflowRequest == null) {
                    request = queue.take();
                    if (request == SENTINEL) {
                        // The epoch increased. Abort this batch
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Discarding end of epoch marker.");
                        }
                        continue;
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
                    // wait for additional requests until either the batch
                    // timeout expires
                    // or the batcher is suspended at least once.
                    request = queue.poll(maxWait, TimeUnit.MILLISECONDS);
                    if (request == null) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Batch timeout");
                        }
                        break;
                    } else if (request == SENTINEL) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Discarding end of epoch marker and partial batch.");
                        }
                        break;
                    } else {
                        if (batchSize + request.byteSize() > maxBatchSize) {
                            // Can't include it in the current batch, as it
                            // would exceed size limit.
                            // Save it for the next batch.
                            overflowRequest = request;
                            break;
                        } else {
                            batchSize += request.byteSize();
                            batchReqs.add(request);
                        }
                    }
                }
                if (request == SENTINEL) {
                    continue;
                }

                // Serialize the batch
                ByteBuffer bb = ByteBuffer.allocate(batchSize);
                bb.putInt(batchReqs.size());
                for (ClientBatch req : batchReqs) {
                    req.writeTo(bb);
                }
                byte[] value = bb.array();
                // Must also pass an array with the request so that the
                // dispatcher thread
                // has enough information for logging the batch
                ClientBatch[] requests = batchReqs.toArray(new ClientBatch[batchReqs.size()]);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Batch ready. Number of requests: " + requests.length +
                                ", queued reqs: " + queue.size());
                }
                // Can block if the Proposer internal propose queue is full
                proposer.enqueueProposal(requests, value);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Batch dispatched.");
                }
            }
        } catch (InterruptedException ex) {
            logger.warning("Thread dying: " + ex.getMessage());
        }
        logger.warning("Thread dying");
    }

    /**
     * Stops the batcher from creating new batches. Called when the process is
     * demoted
     */
    public void suspendBatcher() {
        assert dispatcher.amIInDispatcher();
        if (suspended) {
            // Can happen when the leader advances view before finishing
            // preparing.
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
        try {
            queue.put(SENTINEL);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /** Restarts the batcher, giving it an initial window size. */
    public void resumeBatcher(int currentWndSize) {
        assert dispatcher.amIInDispatcher();
        assert suspended;
        logger.info("Resuming batcher.");
        suspended = false;
    }

    private final static Logger logger =
            Logger.getLogger(ActiveBatcher.class.getCanonicalName());
}
