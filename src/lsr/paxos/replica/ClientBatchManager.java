package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import lsr.common.ClientRequest;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.core.Paxos;
import lsr.paxos.messages.AskForClientBatch;
import lsr.paxos.messages.ForwardClientBatch;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.ClientBatchStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final public class ClientBatchManager {

    private final Network network;
    private final Paxos paxos;
    private final Replica replica;
    private final int localId;

    private final SingleThreadDispatcher dispatcher = new SingleThreadDispatcher(
            "CliBatchManager");
    private final ClientBatchStore batchStore;

    /** Maps missing batch ID to task(s) for retrieving it */
    private final HashMap<ClientBatchID, List<FwdBatchRetransmitter>> missingBatches = new HashMap<ClientBatchID, List<FwdBatchRetransmitter>>();

    private final HashMap<FwdBatchRetransmitter, ScheduledFuture<?>> taskToFuture = new HashMap<ClientBatchManager.FwdBatchRetransmitter, ScheduledFuture<?>>();

    public ClientBatchManager(Paxos paxos, Replica replica) {
        this.paxos = paxos;
        network = paxos.getNetwork();
        this.replica = replica;
        localId = processDescriptor.localId;
        batchStore = ClientBatchStore.instance;

        new InternalMessageHandler();
    }

    public void start() {
        dispatcher.start();
    }

    private class InternalMessageHandler implements MessageHandler {
        public InternalMessageHandler() {
            Network.addMessageListener(MessageType.ForwardedClientBatch, this);
            Network.addMessageListener(MessageType.AskForClientBatch, this);
        }

        public void onMessageReceived(final Message msg, final int sender) {
            dispatcher.submit(new Runnable() {
                public void run() {
                    if (msg instanceof ForwardClientBatch) {
                        onForwardClientBatch((ForwardClientBatch) msg, sender);

                    } else if (msg instanceof AskForClientBatch) {
                        onAskForClientBatch((AskForClientBatch) msg, sender);

                    } else {
                        assert false : "Unknown message type: " + msg;
                    }
                }
            });
        }

        public void onMessageSent(Message message, BitSet destinations) {
            // Ignore
        }
    }

    private void onAskForClientBatch(AskForClientBatch msg, int sender) {
        logger.debug("Received {} from {}", msg, sender);

        for (ClientBatchID cbId : msg.getNeededBatches()) {
            ClientRequest[] batchValue = batchStore.getBatch(cbId);
            if (batchValue != null) {
                logger.debug("Forwarding {} to {}", cbId, sender);
                network.sendMessage(new ForwardClientBatch(cbId, batchValue), sender);
            } else {
                logger.info("Could not deliver requestd batch contents for {}", cbId);
            }
        }
    }

    private void checkIfInDispatcher() {
        assert dispatcher.amIInDispatcher() : "Not in ClientBatchManager dispatcher. " +
                                              Thread.currentThread().getName();
    }

    /**
     * Received a forwarded request.
     */
    private void onForwardClientBatch(ForwardClientBatch fReq, int sender)
    {
        checkIfInDispatcher();

        List<FwdBatchRetransmitter> tasks = missingBatches.remove(fReq.rid);
        if (tasks != null) {
            batchStore.setBatch(fReq);

            for (FwdBatchRetransmitter task : tasks) {
                task.fetched(fReq.rid);
            }

            return;
        }

        if (!usefull(fReq))
            return;

        batchStore.setBatch(fReq);

        tryPropose(fReq.rid);
    }

    /** Returns true iff either required or contain undecided client requests */
    private boolean usefull(ForwardClientBatch fReq) {
        return batchStore.isAnyInstanceWaiting(fReq.rid) ||
               replica.hasUnexecutedRequests(fReq.requests);
    }

    private void tryPropose(ClientBatchID cbId) {
        if (paxos.isLeader())
            paxos.enqueueRequest(cbId);
    }

    /** Transmits a batch to the other replicas */
    public void dispatchForwardNewBatch(final ClientBatchID bid, final ClientRequest[] batches) {
        dispatcher.submit(new Runnable() {
            @Override
            public void run() {
                forwardNewBatch(bid, batches);
            }
        });
    }

    private void forwardNewBatch(ClientBatchID bid, ClientRequest[] batches) {
        checkIfInDispatcher();

        assert processDescriptor.indirectConsensus;
        // The object that will be sent.
        ForwardClientBatch fReqMsg = new ForwardClientBatch(bid, batches);

        logger.debug("Forwarding batch: {}", fReqMsg);

        network.sendToOthers(fReqMsg);
        // Local delivery
        onForwardClientBatch(fReqMsg, localId);
    }

    public static interface Hook {
        void hook();
    }

    public class FwdBatchRetransmitter implements Runnable {
        private final List<ClientBatchID> missing;

        private int nextReplicaToAsk;

        private Hook hook;

        protected FwdBatchRetransmitter(List<ClientBatchID> missing, Hook hook) {
            this.missing = missing;
            this.hook = hook;
            nextReplicaToAsk = processDescriptor.nextReplica(localId);
        }

        public void fetched(ClientBatchID cbId) {
            missing.remove(cbId);
            if (missing.isEmpty())
                finished();
        }

        public void run() {
            checkIfInDispatcher();
            List<ClientBatchID> mine = mine();
            if (mine.isEmpty())
                return;
            network.sendMessage(new AskForClientBatch(mine), nextReplicaToAsk);
            nextReplicaToAsk = processDescriptor.nextReplica(nextReplicaToAsk);
        }

        private List<ClientBatchID> mine() {
            List<ClientBatchID> l = new ArrayList<ClientBatchID>();
            for (ClientBatchID m : missing) {
                assert missingBatches.get(m) != null : missingBatches;
                if (missingBatches.get(m).get(0) == this) {
                    l.add(m);
                }
            }
            return l;
        }

        private void finished() {
            ScheduledFuture<?> sf = taskToFuture.remove(this);
            sf.cancel(false);

            dispatcher.remove(this);
            dispatcher.purge();
            if (hook != null)
                hook.hook();
        }

    }

    /**
     * Fetches batches and calls hook afterwards
     */
    public FwdBatchRetransmitter fetchMissingBatches(final Collection<ClientBatchID> cbids,
                                                     final Hook hook,
                                                     final boolean instant) {
        final FwdBatchRetransmitter[] ans = new FwdBatchRetransmitter[1];
        dispatcher.executeAndWait(new Runnable() {
            @Override
            public void run() {

                List<ClientBatchID> missing = new ArrayList<ClientBatchID>();

                for (ClientBatchID cbId : cbids) {
                    if (cbId.isNop())
                        continue;
                    if (batchStore.getBatch(cbId) == null) {
                        missing.add(cbId);
                    }
                }
                if (missing.isEmpty()) {
                    if (hook != null)
                        hook.hook();
                }

                FwdBatchRetransmitter fbr = new FwdBatchRetransmitter(missing, hook);
                for (ClientBatchID cbId : missing) {
                    if (missingBatches.get(cbId) == null)
                        missingBatches.put(cbId, new ArrayList<FwdBatchRetransmitter>());
                    missingBatches.get(cbId).add(fbr);
                }

                ScheduledFuture<?> sf = dispatcher.scheduleAtFixedRate(fbr, instant ? 0
                        : processDescriptor.retransmitTimeout, processDescriptor.retransmitTimeout /
                                                               processDescriptor.numReplicas,
                        TimeUnit.MILLISECONDS);

                taskToFuture.put(fbr, sf);

                synchronized (ans) {
                    ans[0] = fbr;
                }
            }
        });
        synchronized (ans) {
            return ans[0];
        }
    }

    // TODO: (JK) check if the method below is longer needed
    /** Clears all tasks hanging upon the provided batches */
    public void removeBatches(final Collection<ClientBatchID> cbids) {
        dispatcher.execute(new Runnable() {

            public void run() {
                for (ClientBatchID cbid : cbids) {
                    List<FwdBatchRetransmitter> fbrs = missingBatches.remove(cbid);
                    if (fbrs != null)
                        for (FwdBatchRetransmitter fbr : fbrs)
                        {
                            ScheduledFuture<?> sf = taskToFuture.remove(fbr);
                            if (sf != null) {
                                sf.cancel(false);
                                dispatcher.remove(fbr);
                            }
                        }

                }
                dispatcher.purge();
            }
        });
    }

    public void removeTask(final FwdBatchRetransmitter fbr) {
        dispatcher.executeAndWait(new Runnable() {
            public void run() {
                ScheduledFuture<?> sf = taskToFuture.remove(fbr);
                if (sf == null)
                    return;

                sf.cancel(true);

                dispatcher.remove(fbr);
                dispatcher.purge();
            }
        });
    }

    static final Logger logger = LoggerFactory.getLogger(ClientBatchManager.class);
}
