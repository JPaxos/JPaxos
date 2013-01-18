package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.MovingAverage;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientRequestBatcher;
import lsr.paxos.replica.DecideCallback;

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
     * as many instances as possible, i.e., until either pendingProposals is
     * empty or all the window slots are taken.
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

    private final BlockingQueue<ClientBatchID> queue = new ArrayBlockingQueue<ClientBatchID>(
            MAX_QUEUE_SIZE);

    private ClientBatchID SENTINEL = ClientBatchID.NOP;

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

    public ActiveBatcher(Paxos paxos) {
        this.proposer = (ProposerImpl) paxos.getProposer();
        this.paxosDispatcher = paxos.getDispatcher();
    }

    public void start() {
        batcherThread = new Thread(this, "Batcher");
        batcherThread.setDaemon(true);
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
    public boolean enqueueClientRequest(ClientBatchID request) {
        /*
         * This block is not atomic, so it may happen that suspended is false
         * when the test below is done, but becomes true before this thread has
         * time to put the request in the queue. So some requests might stay in
         * the queue between view changes and be re-proposed later. The request
         * will be ignored, so it does not violate safety. And it should be
         * rare. Avoiding this possibility would require a lock between
         * suspended and put, which would slow down considerably the good case.
         */

        assert !request.equals(SENTINEL);

        if (suspended) {
            logger.warning("Cannot enqueue proposal. Batcher is suspended.");
            return false;
        }
        // This queue should never fill up, the RequestManager.pendingRequests
        // queues will enforce flow control. Use add() instead of put() to throw
        // an exception if the queue fills up.
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
        ArrayList<ClientBatchID> batchReqs = new ArrayList<ClientBatchID>(16);

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
            ClientBatchID overflowRequest = null;

            // Try to build a batch
            while (true) {
                batchReqs.clear();
                // The header takes 4 bytes
                int batchSize = 4;

                ClientBatchID request;
                if (overflowRequest == null) {
                    // (possibly) wait for a new request
                    request = queue.take();
                    if (request == SENTINEL) {
                        // No longer being the leader. Abort this batch
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Discarding end of epoch marker.");
                        }
                        continue;
                    }
                } else {
                    request = overflowRequest;
                    overflowRequest = null;
                }

                averageRequestSize.add(request.byteSize());
                batchSize += request.byteSize();
                batchReqs.add(request);
                // Deadline for sending this batch
                long batchDeadline = System.currentTimeMillis() + processDescriptor.maxBatchDelay;
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Starting batch.");
                }

                // Fill the batch
                while (true) {
                    if (batchSize >= processDescriptor.batchingLevel) {
                        // already full, let's break.
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Batch full");
                        }
                        break;
                    }
                    if (batchSize + (averageRequestSize.get() / 2) >= processDescriptor.batchingLevel) {
                        // small chance to fit the next request.
                        if (queue.isEmpty()) {
                            if (logger.isLoggable(Level.FINER)) {
                                logger.finer("Predicting that next request won't fit. Left with " +
                                             (batchSize - processDescriptor.batchingLevel) +
                                             "bytes, estimated request size:" +
                                             averageRequestSize.get());
                            }
                            break;
                        }
                    }

                    long maxWait = batchDeadline - System.currentTimeMillis();
                    // wait for additional requests until either the batch
                    // timeout expires or the batcher is suspended at least
                    // once.
                    request = queue.poll(maxWait, TimeUnit.MILLISECONDS);
                    if (request == null) {
                        if (decideCallback != null &&
                            decideCallback.hasDecidedNotExecutedOverflow()) {
                            batchDeadline = System.currentTimeMillis() +
                                            Math.max(processDescriptor.maxBatchDelay,
                                                    PRELONGED_BATCHING_TIME);;
                            logger.info("Prelonging batching in ActiveBatcher");
                            continue;
                        } else {
                            if (logger.isLoggable(Level.FINE)) {
                                logger.fine("Batch timeout");
                            }
                            break;
                        }
                    } else if (request == SENTINEL) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Discarding end of epoch marker and partial batch.");
                        }
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
                if (request == SENTINEL) {
                    continue;
                }

                // Serialize the batch
                ByteBuffer bb = ByteBuffer.allocate(batchSize);
                bb.putInt(batchReqs.size());
                for (ClientBatchID req : batchReqs) {
                    req.writeTo(bb);
                }
                byte[] value = bb.array();

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Batch ready. Number of requests: " + batchReqs.size() +
                                ", queued reqs: " + queue.size());
                }
                // Can block if the Proposer internal propose queue is full
                proposer.enqueueProposal(value);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Batch dispatched.");
                }
            }
        } catch (InterruptedException ex) {
            logger.warning("Thread dying: " + ex.getMessage());
        }
        logger.warning("Thread dying");
        throw new RuntimeException("Escaped an ever-lasting loop. should-never-hapen");
    }

    /**
     * Stops the batcher from creating new batches. Called when the process
     * stops being a leader
     */
    public void suspendBatcher() {
        assert paxosDispatcher.amIInDispatcher();
        if (suspended) {
            // Can happen when the leader advances view before finishing
            // preparing.
            return;
        }
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Suspend batcher. Discarding " + queue.size() + " queued requests.");
        }
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

    /** Restarts the batcher, giving it an initial window size. */
    public void resumeBatcher() {
        assert paxosDispatcher.amIInDispatcher();
        assert suspended;
        logger.info("Resuming batcher.");
        suspended = false;
    }

    public void setDecideCallback(DecideCallback decideCallback) {
        this.decideCallback = decideCallback;
    }

    private final static Logger logger =
            Logger.getLogger(ActiveBatcher.class.getCanonicalName());

}
