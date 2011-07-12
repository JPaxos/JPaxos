package lsr.paxos;

import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Accept;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Storage;

/**
 * Represents the part of <code>Paxos</code> which is responsible for receiving
 * <code>Accept</code>. When majority of process send this message it notifies
 * that the value is decided.
 */
class Learner {
    private final Paxos paxos;
    private final Proposer proposer;
    private final Storage storage;

    /**
     * Initializes new instance of <code>Learner</code>.
     * 
     * @param paxos - the paxos the learner belong to
     * @param proposer - the proposer
     * @param storage - data associated with the paxos
     */
    public Learner(Paxos paxos, Storage storage) {
        this.paxos = paxos;
        this.proposer = paxos.getProposer();
        this.storage = storage;
    }

    /**
     * Decides requests from which majority of accepts was received.
     * 
     * @param message - received accept message from sender
     * @param sender - the id of replica that send the message
     * @see Accept
     */
    public void onAccept(Accept message, int sender) {
        assert message.getView() == storage.getView() : "Msg.view: " + message.getView() +
                                                        ", view: " + storage.getView();
        assert paxos.getDispatcher().amIInDispatcher() : "Thread should not be here: " +
                                                         Thread.currentThread();

        ConsensusInstance instance = storage.getLog().getInstance(message.getInstanceId());

        // too old instance or already decided
        if (instance == null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Discarding old accept from " + sender + ":" + message);
            }
            return;
        }
        if (instance.getState() == LogEntryState.DECIDED) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Instance already decided: " + message.getInstanceId());
            }
            return;
        }

        if (message.getView() > instance.getView()) {
            // Reset the instance, the value and the accepts received
            // during the previous view aren't valid on the new view
            logger.fine("Newer accept received " + message);
            instance.reset(message.getView());
        } else {
            // check correctness of received accept
            assert message.getView() == instance.getView();
        }

        instance.getAccepts().set(sender);

        // received ACCEPT before PROPOSE
        if (instance.getValue() == null) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Out of order. Received ACCEPT before PROPOSE. Instance: " + instance);
            }
        }

        if (paxos.isLeader()) {
            proposer.stopPropose(instance.getId(), sender);
        }

        if (instance.isMajority(ProcessDescriptor.getInstance().numReplicas)) {
            if (instance.getValue() == null) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Majority but no value. Delaying deciding. Instance: " + instance.getId());
                }
            } else {
                paxos.decide(instance.getId());
            }
        }
    }

    private final static Logger logger = Logger.getLogger(Learner.class.getCanonicalName());
}