package lsr.paxos.replica;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientCommand.CommandType;
import lsr.common.ClientReply;
import lsr.common.ClientRequest;
import lsr.common.MovingAverage;
import lsr.common.RequestId;
import lsr.common.SingleThreadDispatcher;

public class InternalClient {

    private static final int FIRST_AVERAGE = 500;

    MovingAverage averageRequestTime = new MovingAverage(0.3, FIRST_AVERAGE);

    final SingleThreadDispatcher replicaDispatcher;
    final ClientRequestManager clientRequestManager;

    final Object idLock = new Object();

    final ConcurrentLinkedQueue<RequestId> freeIds = new ConcurrentLinkedQueue<RequestId>();

    public InternalClient(SingleThreadDispatcher replicaDispatcher,
                          ClientRequestManager clientRequestManager) {
        this.replicaDispatcher = replicaDispatcher;
        this.clientRequestManager = clientRequestManager;
    }

    /**
     * @throws InterruptedException in case the replica has been interrupted
     *             while waiting to put the request on propose queue
     */
    public void executeNonFifo(byte[] request) {
        RequestId reqId = freeIds.poll();
        if (reqId == null)
            reqId = new RequestId(NioClientProxy.idGenerator.next(), 0);

        ClientRequest cr = new ClientRequest(reqId, request);
        ClientCommand cc = new ClientCommand(CommandType.REQUEST, cr);

        InternalClientProxy icp = new InternalClientProxy(reqId);

        RequestRepeater rr = new RequestRepeater(cc, icp);

        icp.setRepeater(rr, replicaDispatcher.schedule(rr,
                (long) (3 * averageRequestTime.get()),
                TimeUnit.MILLISECONDS));

        if (logger.isLoggable(Level.FINE))
            logger.fine("InternalClient proposes: " + reqId);

        clientRequestManager.dispatchOnClientRequest(cc, icp);
    }

    protected class InternalClientProxy implements ClientProxy {

        private final long cliId;
        private final int seqNo;
        private RequestRepeater repeater;
        private ScheduledFuture<?> sf;
        private final long startTime = System.currentTimeMillis();

        public InternalClientProxy(RequestId reqId) {
            cliId = reqId.getClientId();
            seqNo = reqId.getSeqNumber();
        }

        /** Called upon generating the answer for previous request */
        public void send(ClientReply clientReply) {
            if (logger.isLoggable(Level.FINE))
                logger.fine("InternalClient completed " + cliId + ":" + seqNo);
            averageRequestTime.add(System.currentTimeMillis() - startTime);
            freeIds.add(new RequestId(cliId, seqNo + 1));
            replicaDispatcher.remove(repeater);
            sf.cancel(false);
        }

        public void setRepeater(RequestRepeater repeater, ScheduledFuture<?> sf) {
            this.repeater = repeater;
            this.sf = sf;
        }
    }

    protected class RequestRepeater implements Runnable {

        private final ClientCommand cc;
        private final InternalClientProxy icp;

        public RequestRepeater(ClientCommand cc, InternalClientProxy icp) {
            this.cc = cc;
            this.icp = icp;
        }

        public void run() {
            if (!shouldRepeat())
                return;

            if (logger.isLoggable(Level.FINE))
                logger.fine("InternalClient re-proposes: " + cc.getRequest().getRequestId());

            icp.setRepeater(this, replicaDispatcher.schedule(this,
                    (long) (3 * averageRequestTime.get()),
                    TimeUnit.MILLISECONDS));

            clientRequestManager.dispatchOnClientRequest(cc, icp);
        }

        private boolean shouldRepeat() {
            // FIXME: (JK) check if the request deciding is in progress
            /*
             * The problem is that tracing each request is not easy,
             */
            return true;
        }
    }

    private final static Logger logger = Logger.getLogger(InternalClient.class.getCanonicalName());
}
