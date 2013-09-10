package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import lsr.common.ClientRequest;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.common.RequestType;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.AugmentedBatch.BatchId;
import lsr.paxos.core.Paxos;
import lsr.paxos.core.ProposerImpl;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.DecideCallback;
import lsr.paxos.replica.Replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AugmentedBatcher implements Batcher, Runnable {

    private final static int MAX_REQ_QUEUE_SIZE = 10 * 1024;

    private final BlockingQueue<RequestType> reqQueue = new ArrayBlockingQueue<RequestType>(
            MAX_REQ_QUEUE_SIZE);

    private ClientBatchID SENTINEL = ClientBatchID.NOP;
    private static RequestType WAKE_UP = new RequestType() {
        @Override
        public void writeTo(ByteBuffer bb) {
        }

        @Override
        public int byteSize() {
            return 0;
        }
    };

    private volatile boolean suspended = true;
    private int lastInstanceDelivered = -1;
    private AugmentedBatch.BatchId lastBatchDelivered = BatchId.startBatch;

    private final Replica replica;
    private final SingleThreadDispatcher paxosDispatcher;
    private final ProposerImpl proposer;
    private Thread batcherThread;

    private final Map<Long, Reply> executedRequests;
    private final Set<RequestId> leaderDeliveredRequests = new HashSet<RequestId>();

    private boolean isLeader = false;
    private int leaderReign = 0;
    private int lastBatchLeader = -1;
    private int lastBatchReign = -1;

    public AugmentedBatcher(Replica replica, Paxos paxos)
    {
        this.replica = replica;
        this.paxosDispatcher = paxos.getDispatcher();
        this.proposer = (ProposerImpl) paxos.getProposer();
        this.executedRequests = replica.getExecutedRequestsMap();
    }

    public void start() {
        batcherThread = new Thread(this, "Batcher");
        batcherThread.setDaemon(true);
        batcherThread.start();
    }

    @Override
    public void enqueueClientRequest(final RequestType request) {
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
            reqQueue.put(request);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] requestBatch() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void suspendBatcher() {
        // TODO Auto-generated method stub
    }

    @Override
    public void resumeBatcher(int nextInstanceId) {
        if (lastInstanceDelivered == nextInstanceId - 1)
            ;
        // TODO Auto-generated method stub
    }

    @Override
    public void instanceExecuted(int instanceId, AugmentedBatch augmentedBatch) {
        lastInstanceDelivered = instanceId;

        if (!augmentedBatch.getPreviousBatchId().equals(lastBatchDelivered))
            return;

        if (isLeader && augmentedBatch.getBatchId().leader != processDescriptor.localId)
        {
            // TODO revoke leadership, deliver, gain leadership
        }
        else
        {
            // TODO: put new D event to events queue
        }
    }

    @Override
    public void run() {

        while (true) {
            // TODO: serialize with instanceExecuted, suspend/resumeBatcher,
            // abcast, requestBatch
            // TODO: get something from reqQueue, cast it to ClientRequest
            ClientRequest req = null;

            if (!checkRequest(req))
                continue;

            // TODO: put new LD event to events queue
            leaderDeliveredRequests.add(req.getRequestId());
        }
    }

    private boolean checkRequest(ClientRequest request) {
        RequestId id = request.getRequestId();
        Reply lastReply = executedRequests.get(id.getClientId());
        if (lastReply != null && lastReply.getRequestId().getSeqNumber() >= id.getSeqNumber())
            return false;
        return !leaderDeliveredRequests.contains(id);
    }

    private final static Logger logger = LoggerFactory.getLogger(AugmentedBatcher.class);

    @Override
    public void setDecideCallback(DecideCallback decideCallback) {
        /*
         * TODO: (JK) decided callback has method:
         * 
         * decideCallback.hasDecidedNotExecutedOverflow()
         * 
         * that tells if there are many decided & undelivered requests. If there
         * are, batcher may progress slower.
         */

    }
}
