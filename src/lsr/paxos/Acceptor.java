package lsr.paxos;

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Accept;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
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

        // TODO:
        // If the message is from the current view:
        // a) This process already received a proposal from this view ->
        // then the proposer already completed the prepare phase and doesn't
        // need the PrepareOK.
        // b) No proposal was received for current view - then this is a
        // retransmission. Must
        //
        // Currently: always retransmit. The proposer will ignore old
        // PrepareOK messages.
        // Do not send message, since it can be quite large in some
        // cases.
        // Possible implementations:
        // - check if the topmost entry on the log is already stamped with
        // the current view.
        // - Keep a flag associated with the view indicating if a proposal
        // was already received for the current view.
        if (logger.isLoggable(Level.WARNING)) {
            logger.warning(msg.toString());
        }

        Log log = storage.getLog();

        if (msg.getFirstUncommitted() < log.getLowestAvailableId()) {
            // We're MUCH MORE up-to-date than the replica that sent Prepare
            paxos.startProposer();
            return;
        }

        ConsensusInstance[] v = new ConsensusInstance[Math.max(
                log.getNextId() - msg.getFirstUncommitted(), 0)];
        for (int i = msg.getFirstUncommitted(); i < log.getNextId(); i++) {
            v[i - msg.getFirstUncommitted()] = log.getInstance(i);
        }

        /* TODO: FullSS. Sync view number. 
         * Promise not to accept a phase 1a message for view v.
         */
        PrepareOK m = new PrepareOK(msg.getView(), v, storage.getEpoch());
        if (logger.isLoggable(Level.WARNING)) {
            logger.warning("Sending " + m);
        }
        network.sendMessage(m, sender);
    }

    /**
     * Accepts proposals higher or equal than the current view.
     * 
     * @param message - received propose message
     * @param sender - the id of replica that send the message
     */
    public void onPropose(Propose message, int sender) {
        // TODO: What if received a proposal for a higher view?
        assert message.getView() == storage.getView() : "Msg.view: " + message.getView() +
                ", view: " + storage.getView();
        assert paxos.getDispatcher().amIInDispatcher() : "Thread should not be here: " +
        Thread.currentThread();
        ConsensusInstance instance = storage.getLog().getInstance(message.getInstanceId());
        // The propose is so old, that it's log has already been erased
        if (instance == null) {
            logger.fine("Ignoring old message: " + message);
            return;
        }

        instance.setValue(message.getView(), message.getValue());
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("onPropose. View:instance: " + message.getView() + ":" + message.getInstanceId());
        }


        ProcessDescriptor descriptor = ProcessDescriptor.getInstance();

        // leader will not send the accept message;
        if (!paxos.isLeader()) {
            // TODO: (JK) Is this what we want? They'll catch up later, and the
            // leader can respond faster to clients

            // FIXME: Flow control is disabled.
            // Do not send ACCEPT if there are old instances unresolved
            //            int firstUncommitted = storage.getFirstUncommitted();
            //            int wndSize = ProcessDescriptor.getInstance().windowSize;
            //            if (firstUncommitted + wndSize < message.getInstanceId()) {
            //                logger.info("Instance " + message.getInstanceId() + " out of window.");
            //
            //                if (firstUncommitted + wndSize * 2 < message.getInstanceId()) {
            //                    // Assume that message is lost. Execute catchup with normal
            //                    // priority
            //                    paxos.getCatchup().forceCatchup();
            //                } else {
            //                    // Message may not be lost, but try to execute catchup if
            //                    // idle
            //                    paxos.getCatchup().startCatchup();
            //                }
            //
            //            } else {

            try {
                // Slow down the acceptor, to give the leader a chance of starting multiple parallel instances
                Thread.sleep(5);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            /*
             * TODO: NS [FullSS] Save to stable storage <Accept, view,
             * instance, value> Must not accept a different value for the
             * same pair of view and instance.
             */
            // Do not send ACCEPT to self
            network.sendToOthers(new Accept(message));
            //            }
        }

        // Might have enough accepts to decide.
        if (instance.getState() == LogEntryState.DECIDED) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.fine("Instance already decided: " + message.getInstanceId());
            }
        } else {
            // The local process accepts immediately the proposal,
            // avoids sending an accept message.
            instance.getAccepts().set(descriptor.localId);
            // The propose message works as an implicit accept from the leader.
            instance.getAccepts().set(sender);
            if (instance.isMajority(descriptor.numReplicas)) {
                paxos.decide(instance.getId());
            }
        }
    }

    private final static Logger logger = Logger.getLogger(Acceptor.class.getCanonicalName());
}
