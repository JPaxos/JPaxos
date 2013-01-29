package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.Deque;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.paxos.Batcher;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientBatchManager;
import lsr.paxos.replica.ClientBatchManager.FwdBatchRetransmitter;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

/**
 * Represents part of paxos which is responsible for responding on the
 * <code>Prepare</code> message, and also sending <code>Accept</code> after
 * receiving proper <code>Propose</code>.
 */
class Acceptor {
    private final Paxos paxos;
    private final Storage storage;
    private final Network network;

    /**
     * Initializes new instance of <code>Acceptor</code>.
     * 
     * @param paxos - the paxos the acceptor belong to
     * @param storage - data associated with the paxos
     * @param network - used to send responses
     * 
     */
    public Acceptor(Paxos paxos, Storage storage, Network network) {
        this.paxos = paxos;
        this.storage = storage;
        this.network = network;
    }

    /**
     * Promises not to accept a proposal numbered less than message view. Sends
     * the proposal with the highest number less than message view that it has
     * accepted if any. If message view equals current view, then it may be a
     * retransmission or out-of-order delivery. If the process already accepted
     * this proposal, then the proposer doesn't need anymore the prepareOK
     * message. Otherwise it might need the message, so resent it.
     * 
     * @param msg received prepare message
     * @see Prepare
     */
    public void onPrepare(Prepare msg, int sender) {
        assert paxos.getDispatcher().amIInDispatcher() : "Thread should not be here: " +
                                                         Thread.currentThread();

        // TODO: JK: When can we skip responding to a prepare message?
        // Is detecting stale prepare messages it worth it?

        if (logger.isLoggable(Level.WARNING)) {
            logger.warning(msg.toString() + " From " + sender);
        }

        Log log = storage.getLog();

        if (msg.getFirstUncommitted() < log.getLowestAvailableId()) {
            // We're MUCH MORE up-to-date than the replica that sent Prepare
            if (paxos.isActive())
                paxos.startProposer();
            return;
        }

        ConsensusInstance[] v = new ConsensusInstance[Math.max(
                log.getNextId() - msg.getFirstUncommitted(), 0)];
        for (int i = msg.getFirstUncommitted(); i < log.getNextId(); i++) {
            v[i - msg.getFirstUncommitted()] = log.getInstance(i);
        }

        PrepareOK m = new PrepareOK(msg.getView(), v, storage.getEpoch());
        if (logger.isLoggable(Level.WARNING)) {
            logger.warning("Sending " + m);
        }

        if (paxos.isActive())
            network.sendMessage(m, sender);
    }

    /**
     * Accepts proposals higher or equal than the current view.
     * 
     * @param message - received propose message
     * @param sender - the id of replica that send the message
     */
    public void onPropose(final Propose message, final int sender) {
        assert message.getView() == storage.getView() : "Msg.view: " + message.getView() +
                                                        ", view: " + storage.getView();
        assert paxos.getDispatcher().amIInDispatcher();

        ConsensusInstance instance = storage.getLog().getInstance(message.getInstanceId());
        // The propose is so old, that it's log has already been erased
        if (instance == null) {
            logger.fine("Ignoring old message: " + message);
            return;
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("onPropose. View:instance: " + message.getView() + ":" +
                        message.getInstanceId());
        }

        Deque<ClientBatchID> cbids = Batcher.unpack(message.getValue());

        // leader must have the values
        if (!paxos.isLeader()) {

            // as follower, we may be missing the real value. If so, need to
            // wait for it.

            if (!ClientBatchStore.instance.hasAllBatches(cbids)) {
                logger.info("Missing batch values for instance " + instance.getId() +
                            ". Delaying onPropose.");
                FwdBatchRetransmitter fbr = ClientBatchStore.instance.getClientBatchManager().fetchMissingBatches(
                        cbids,
                        new ClientBatchManager.Hook() {

                            @Override
                            public void hook() {
                                paxos.getDispatcher().execute(new Runnable() {
                                    public void run() {
                                        onPropose(message, sender);
                                    }
                                });
                            }
                        }, false);
                instance.setFwdBatchForwarder(fbr);

                return;
            }
        }

        // In FullSS, updating state leads to setting new value if needed, which
        // syncs to disk
        instance.updateStateFromKnown(message.getView(), message.getValue());

        // prevent multiple unpacking
        instance.setClientBatchIds(cbids);

        assert instance.getValue() != null;

        // leader will not send the accept message;
        if (!paxos.isLeader()) {

            if (storage.getFirstUncommitted() + (processDescriptor.windowSize * 3) < message.getInstanceId()) {
                // the instance is so new that we must be out of date.
                paxos.getCatchup().forceCatchup();
            }

            if (paxos.isActive())
                network.sendToOthers(new Accept(message));
        }

        // we could have decided the instance earlier
        if (instance.getState() == LogEntryState.DECIDED) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.fine("Instance already decided: " + message.getInstanceId());
            }
            return;
        }

        // The local process accepts immediately the proposal
        instance.getAccepts().set(processDescriptor.localId);
        // The propose message works as an implicit accept from the leader.
        instance.getAccepts().set(sender);

        // Check if we can decide (n<=3 or if some accepts overtook propose)
        if (instance.isMajority()) {
            paxos.decide(instance.getId());
        }
    }

    private final static Logger logger = Logger.getLogger(Acceptor.class.getCanonicalName());
}
