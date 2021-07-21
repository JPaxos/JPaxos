package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.paxos.messages.Accept;
import lsr.paxos.storage.ClientBatchStore;
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

        boolean isReadyToBeDecided = instance.updateStateFromAccept(message.getView(), sender);

        if (paxos.isLeader()) {
            proposer.stopPropose(instance.getId(), sender);
        }

        if (isReadyToBeDecided) {
            assert !processDescriptor.indirectConsensus ||
                   ClientBatchStore.instance.hasAllBatches(instance.getClientBatchIds());
            paxos.decide(instance.getId());
        } else {
            logger.trace("Not enough accepts for {} yet", instance.getId());
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(Learner.class);
}