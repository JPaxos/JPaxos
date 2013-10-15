package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;
import lsr.paxos.messages.Accept;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        final ConsensusInstance instance = storage.getLog().getInstance(message.getInstanceId());

        logger.trace("Learner received {}", message);
        
        // too old instance or already decided
        if (instance == null) {
            logger.info("Discarding old accept from {}:{}", sender, message);
            return;
        }

        if (instance.getState() == LogEntryState.DECIDED) {
            if (logger.isDebugEnabled())
                logger.debug("Ignoring Accept. Instance already decided: {}",
                        message.getInstanceId());
            return;
        }

        if (instance.getView() == -1) {
            assert instance.getAccepts().isEmpty() : "First message for instance but accepts not empty: " +
                                                     instance;
            // This is the first message received for this instance. Set the
            // view.
            instance.setView(message.getView());

        } else if (message.getView() > instance.getView()) {
            // Reset the instance, the value and the accepts received
            // during the previous view aren't valid on the new view
            logger.debug("Accept for higher view received. Rcvd: {}, instance: {}", message,
                    instance);
            instance.reset();
            instance.setView(message.getView());

        } else {
            // check correctness of received accept
            assert message.getView() == instance.getView();
        }

        instance.getAccepts().set(sender);

        // received ACCEPT before PROPOSE
        if (instance.getValue() == null) {
            logger.debug("Out of order. Received ACCEPT before PROPOSE. Instance: {}", instance);
        }

        if (paxos.isLeader()) {
            proposer.stopPropose(instance.getId(), sender);
        }

        if (instance.isMajority()) {
            if (instance.getValue() == null) {
                logger.debug("Majority but no value. Delaying deciding. Instance: {}",
                        instance.getId());
            } else {
                assert !processDescriptor.indirectConsensus
                       || ClientBatchStore.instance.hasAllBatches(instance.getClientBatchIds());
                paxos.decide(instance.getId());
            }
        } else {
            logger.trace("Not enough accepts for {} yet, got {}", instance.getId(),
                    instance.getAccepts());
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(Learner.class);
}