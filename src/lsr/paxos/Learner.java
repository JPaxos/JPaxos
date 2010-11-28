package lsr.paxos;

import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.paxos.messages.Accept;
import lsr.paxos.network.Network;
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
     * @param storage - data associated with the paxos
     */
    public Learner(Paxos paxos, Proposer proposer, Storage storage, Network network) {
        this.paxos = paxos;
        this.proposer = proposer;
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
        assert message.getView() == storage.getStableStorage().getView() : "Msg.view: " +
                                                                           message.getView() +
                                                                           ", view: " +
                                                                           storage.getStableStorage().getView();
        assert paxos.getDispatcher().amIInDispatcher() : "Thread should not be here: " +
                                                         Thread.currentThread();

        ConsensusInstance instance = storage.getLog().getInstance(message.getInstanceId());

        // too old instance or already decided
        if (instance == null) {
            if (_logger.isLoggable(Level.INFO)) {
                _logger.info("Discarding old accept from " + sender + ":" + message);
            }
            return;
        }
        if (instance.getState() == LogEntryState.DECIDED) {
            // _logger.warning("Duplicate accept? " + message.getInstanceId() +
            // " from " + sender);
            if (_logger.isLoggable(Level.FINEST)) {
                _logger.fine("Instance already decided: " + message.getInstanceId());
            }
            return;
        }

        if (message.getView() > instance.getView()) {
            // Reset the instance, the value and the accepts received
            // during the previous view aren't valid on the new view
            _logger.fine("Newer accept received " + message);
            instance.getAccepts().clear();
            instance.setValue(message.getView(), null);
        } else {
            // check correctness of received accept
            assert message.getView() == instance.getView();
            // assert Arrays.equals(message.getValue(), instance.getValue()) :
            // "Different instance value for the same view: "
            // + instance.getValue() + ", msg: " + message.getValue();
        }

        instance.getAccepts().set(sender);

        // received ACCEPT before PROPOSE
        if (instance.getValue() == null) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Out of order. Received ACCEPT before PROPOSE. Instance: " + instance);
            }
            // _network.sendMessage(message, _storage.getAcceptors());
        }

        if (paxos.isLeader()) {
            proposer.stopPropose(instance.getId(), sender);
        }

        if (instance.isMajority(storage.getN())) {
            if (instance.getValue() == null) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("Majority but no value. Delaying deciding. Instance: " +
                                 instance.getId());
                }
            } else {
                paxos.decide(instance.getId());
            }
        }
    }

    private final static Logger _logger = Logger.getLogger(Learner.class.getCanonicalName());
}