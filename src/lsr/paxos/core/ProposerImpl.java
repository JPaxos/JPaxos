package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.CrashModel;
import lsr.paxos.ActiveRetransmitter;
import lsr.paxos.EpochPrepareRetransmitter;
import lsr.paxos.FailureDetector;
import lsr.paxos.PrepareRetransmitter;
import lsr.paxos.PrepareRetransmitterImpl;
import lsr.paxos.RetransmittedMessage;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientBatchManager;
import lsr.paxos.replica.ClientRequestManager;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

/**
 * Represents part of paxos which is responsible for proposing new consensus
 * values. Provides procedures to start proposing which sends the
 * {@link Propose} messages, and allows proposing new values. The number of
 * currently running proposals is defined by <code>MAX_ACTIVE_PROPOSALS</code>.
 */
public class ProposerImpl implements Proposer {

    /** retransmitted message for prepare request */
    private PrepareRetransmitter prepareRetransmitter;

    /** retransmitted propose messages for instances */
    private final Map<Integer, RetransmittedMessage> proposeRetransmitters =
            new HashMap<Integer, RetransmittedMessage>();

    /** Keeps track of the processes that have prepared for this view */
    private final ActiveRetransmitter retransmitter;
    private final Paxos paxos;
    private final Storage storage;
    private final FailureDetector failureDetector;

    private ProposerState state;

    private ClientBatchManager cliBatchManager;

    /** Locked on the array, modifies the int inside. */
    private final int[] waitingHooks = new int[] {0};

    /**
     * Initializes new instance of <code>Proposer</code>. If the id of current
     * replica is 0 then state is set to <code>ACTIVE</code>. Otherwise
     * <code>INACTIVE</code> state is set.
     * 
     * @param paxos - the paxos the acceptor belong to
     * @param network - data associated with the paxos
     * @param failureDetector - used to notify about leader change
     * @param storage - used to send responses
     */
    public ProposerImpl(Paxos paxos,
                        Network network,
                        FailureDetector failureDetector,
                        Storage storage,
                        CrashModel crashModel)
    {
        this.paxos = paxos;
        this.failureDetector = failureDetector;
        this.storage = storage;
        retransmitter = new ActiveRetransmitter(network, "ProposerRetransmitter");

        this.state = ProposerState.INACTIVE;

        if (crashModel == CrashModel.EpochSS) {
            prepareRetransmitter = new EpochPrepareRetransmitter(retransmitter, storage);
        } else {
            prepareRetransmitter = new PrepareRetransmitterImpl(retransmitter);
        }
    }

    public void setClientRequestManager(ClientRequestManager requestManager) {
        cliBatchManager = requestManager.getClientBatchManager();
    }

    public void start() {
        assert cliBatchManager != null;
        retransmitter.init();
    }

    /**
     * Gets the current state of the proposer.
     * 
     * @return <code>ACTIVE</code> if the current proposer can propose new
     *         values, <code>INACTIVE</code> otherwise
     */
    public ProposerState getState() {
        return state;
    }

    /**
     * If previous leader is suspected this procedure is executed. We're
     * changing the view (variable indicating order of the leaders in time)
     * accordingly, and we're sending the prepare message.
     * 
     */
    public void prepareNextView() {
        assert paxos.getDispatcher().amIInDispatcher();
        assert state == ProposerState.INACTIVE : "Proposer is ACTIVE.";

        state = ProposerState.PREPARING;
        setNextViewNumber();
        failureDetector.viewChange(storage.getView());

        logger.warning("Preparing view: " + storage.getView());

        Prepare prepare = new Prepare(storage.getView(), storage.getFirstUncommitted());
        prepareRetransmitter.startTransmitting(prepare, Network.OTHERS);

        fetchLocalMissingBatches();

        // tell that local process is already prepared
        prepareRetransmitter.update(null, processDescriptor.localId);
        // unlikely, unless N==1
        if (prepareRetransmitter.isMajority()) {
            onMajorityOfPrepareOK();
        }
    }

    private void fetchLocalMissingBatches() {

        for (ConsensusInstance instane : storage.getLog().getInstanceMap().tailMap(
                storage.getFirstUncommitted()).values()) {
            if (instane.getState() == LogEntryState.KNOWN &&
                !ClientBatchStore.instance.hasAllBatches(instane)) {
                waitingHooks[0]++;
                cliBatchManager.fetchMissingBatches(instane, new ClientBatchManager.Hook() {

                    public void hook(ConsensusInstance ci) {
                        synchronized (waitingHooks) {
                            waitingHooks[0]--;
                            waitingHooks.notifyAll();
                        }

                    }
                }, true);
            }
        }
    }

    private void setNextViewNumber() {
        int view = storage.getView();
        do {
            view++;
        } while (view % processDescriptor.numReplicas != processDescriptor.localId);
        storage.setView(view);
    }

    public void onPrepareOK(PrepareOK message, int sender) {
        assert paxos.getDispatcher().amIInDispatcher();
        assert paxos.isLeader();
        assert state != ProposerState.INACTIVE : "Proposer is not active.";

        // asserting the same again. Who knows what happens in between?
        assert message.getView() == storage.getView() : "Received a PrepareOK for a higher or lower view. " +
                                                        "Msg.view: " + message.getView() +
                                                        ", view: " + storage.getView();

        logger.info("Received from " + sender + ": " + message);

        // Ignore prepareOK messages if we have finished preparing
        if (state == ProposerState.PREPARED) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("View " + storage.getView() +
                            " already prepared. Ignoring message.");
            }
            return;
        }

        updateLogFromPrepareOk(message);
        prepareRetransmitter.update(message, sender);

        if (prepareRetransmitter.isMajority()) {
            onMajorityOfPrepareOK();
        }
    }

    private void onMajorityOfPrepareOK() {
        prepareRetransmitter.stop();

        logger.info("Majority of PrepareOK gathered. Waiting for " + waitingHooks[0] +
                    " missing batch values");

        // wait for all batch values to arrive
        synchronized (waitingHooks) {
            while (waitingHooks[0] > 0)
                try {
                    waitingHooks.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
        }

        state = ProposerState.PREPARED;

        logger.warning("View prepared " + storage.getView());

        // Send a proposal for all instances that were not decided.
        Log log = storage.getLog();
        for (int i = storage.getFirstUncommitted(); i < log.getNextId(); i++) {
            ConsensusInstance instance = log.getInstance(i);
            assert instance != null;
            switch (instance.getState()) {
                case DECIDED:
                    // If the decision was already taken by some process,
                    // there is no need to propose again, so skip this
                    // instance
                    break;

                case KNOWN:
                    // No decision, but some value is known
                    logger.info("Proposing value from previous view: " + instance);
                    instance.setView(storage.getView());
                    continueProposal(instance);
                    break;

                case UNKNOWN:
                    assert instance.getValue() == null : "Unknow instance has value";
                    logger.info("No value locked for instance " + i + ": proposing no-op");
                    fillWithNoOperation(instance);
                    break;

                default:
                    assert false;
            }
        }

        synchronized (pendingProposals) {
            acceptNewBatches = true;
            pendingProposals.notify();
        }

        paxos.onViewPrepared();

        enqueueOrphanedBatches();
    }

    private void enqueueOrphanedBatches() {
        HashSet<ClientBatchID> instanceless = ClientBatchStore.instance.getInstancelessBatches();
        for (ClientBatchID cbid : instanceless)
            paxos.enqueueRequest(cbid);
    }

    private void fillWithNoOperation(ConsensusInstance instance) {
        ByteBuffer bb = ByteBuffer.allocate(4 + ClientBatchID.NOP.byteSize());
        bb.putInt(1); // Size of batch
        ClientBatchID.NOP.writeTo(bb); // request
        instance.updateStateFromKnown(storage.getView(), bb.array());
        continueProposal(instance);
    }

    private void updateLogFromPrepareOk(PrepareOK message) {
        if (message.getPrepared() == null) {
            return;
        }

        // Update the local log with the data sent by this process
        for (final ConsensusInstance ci : message.getPrepared()) {

            // Algorithm: The received instance can be either
            // Decided - Set the local log entry to decided.
            // Accepted - If the local log entry is decided, ignore.
            // Otherwise, find the accept message for this consensus
            // instance with the highest timestamp and propose it.
            final ConsensusInstance localLog = storage.getLog().getInstance(ci.getId());
            // Happens if previous PrepareOK caused a snapshot execution
            if (localLog == null) {
                continue;
            }

            if (localLog.getState() == LogEntryState.DECIDED) {
                // We already know the decision, so ignore it.
                continue;
            }
            switch (ci.getState()) {
                case DECIDED:
                    localLog.updateStateFromDecision(ci.getView(), ci.getValue());
                    if (ClientBatchStore.instance.hasAllBatches(ci)) {
                        paxos.decide(ci.getId());
                    } else {
                        waitingHooks[0]++;
                        cliBatchManager.fetchMissingBatches(ci, new ClientBatchManager.Hook() {

                            public void hook(final ConsensusInstance ci) {
                                paxos.getDispatcher().executeAndWait(new Runnable() {
                                    public void run() {
                                        paxos.decide(ci.getId());
                                    }
                                });
                                synchronized (waitingHooks) {
                                    waitingHooks[0]--;
                                    waitingHooks.notifyAll();
                                }
                            }
                        }, true);
                    }
                    break;

                case KNOWN:
                    assert ci.getValue() != null : "Instance state KNOWN but value is null";
                    localLog.updateStateFromKnown(ci.getView(), ci.getValue());
                    if (!ClientBatchStore.instance.hasAllBatches(ci)) {
                        waitingHooks[0]++;
                        cliBatchManager.fetchMissingBatches(ci, new ClientBatchManager.Hook() {

                            public void hook(ConsensusInstance ci) {
                                synchronized (waitingHooks) {
                                    waitingHooks[0]--;
                                    waitingHooks.notifyAll();
                                }
                            }
                        }, true);
                    }
                    break;

                case UNKNOWN:
                    assert ci.getValue() == null : "Unknow instance has value";
                    logger.fine("Ignoring: " + ci);
                    break;

                default:
                    assert false : "Invalid state: " + ci.getState();
                    break;
            }
        }

    }

    final class Proposal implements Runnable {
        final byte[] value;

        public Proposal(byte[] value) {
            this.value = value;
        }

        @Override
        public void run() {
            assert paxos.getDispatcher().amIInDispatcher();
            logger.fine("Propose task running");
            proposeNext();
        }
    }

    private final static int MAX_QUEUED_PROPOSALS = 30;
    private final Deque<Proposal> pendingProposals = new ArrayDeque<Proposal>();
    /*
     * Condition variable. Ensures that the batcher thread only enqueues new
     * batches if the local process is on the leader role and its view is
     * prepared. Used to prevent batches from being left forgotten on the
     * pendingProposals queue when the process is not the leader.
     */
    private boolean acceptNewBatches = false;

    public void enqueueProposal(byte[] value)
            throws InterruptedException
    {
        // Called from batcher thread
        Proposal proposal = new Proposal(value);
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("pendingProposals.size() = " + pendingProposals.size() + ", MAX: " +
                        MAX_QUEUED_PROPOSALS);
        }
        // Block until there is space for adding new batches
        synchronized (pendingProposals) {
            while (pendingProposals.size() > MAX_QUEUED_PROPOSALS && acceptNewBatches) {
                pendingProposals.wait();
            }

            // Ignore the batch if the proposer became inactive
            if (!acceptNewBatches) {
                logger.fine("Proposer not active.");
                return;
            }

            boolean wasEmpty = pendingProposals.isEmpty();

            pendingProposals.addLast(proposal);

            if (wasEmpty) {
                logger.info("Scheduling proposal task");
                try {
                    paxos.getDispatcher().submit(proposal);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Enqueued proposal. pendingProposal.size(): " + pendingProposals.size());
        }
    }

    public void proposeNext() {
        if (logger.isLoggable(Level.FINE)) {
            logger.info("Proposing. pendingProposals.size(): " + pendingProposals.size() +
                        ", window used: " + storage.getWindowUsed());
        }
        while (!storage.isWindowFull()) {
            Proposal proposal;
            synchronized (pendingProposals) {
                if (pendingProposals.isEmpty()) {
                    return;
                }
                proposal = pendingProposals.pop();
                // notify only if the queue is getting empty
                if (pendingProposals.size() < MAX_QUEUED_PROPOSALS / 2) {
                    pendingProposals.notify();
                }
            }
            propose(proposal.value);
        }
    }

    /**
     * Asks the proposer to propose the given value. If there are currently too
     * many active propositions, this proposal will be enqueued until there are
     * available slots. If the proposer is <code>INACTIVE</code>, then message
     * is discarded. Otherwise value is added to list of active proposals.
     * 
     * @param value - the value to propose
     * @throws InterruptedException
     */
    public void propose(byte[] value) {
        assert paxos.getDispatcher().amIInDispatcher();
        if (state != ProposerState.PREPARED) {
            /*
             * This can happen if there is a Propose event queued on the
             * Dispatcher when the view changes.
             */
            logger.warning("Cannot propose in INACTIVE or PREPARING state. Discarding batch");
            return;
        }

        if (logger.isLoggable(Level.INFO)) {
            /** Builds the string with the log message */
            StringBuilder sb = new StringBuilder(64);
            sb.append("Proposing: ").append(storage.getLog().getNextId()).append(", Size:");
            logger.info(sb.toString());
        }

        ConsensusInstance instance = storage.getLog().append(storage.getView(), value);

        assert ClientBatchStore.instance.hasAllBatches(instance);

        // Mark the instance as accepted locally
        instance.getAccepts().set(processDescriptor.localId);

        RetransmittedMessage msg = retransmitter.startTransmitting(new Propose(instance));
        proposeRetransmitters.put(instance.getId(), msg);
    }

    /**
     * Called to inform the proposer that a decision was taken. Allows the
     * proposer to make a new proposal.
     */
    public void ballotFinished() {
        assert paxos.getDispatcher().amIInDispatcher();

        // Needed - decide (triggering this method) is i.a. called by PrepareOK
        if (state == ProposerState.PREPARED) {
            proposeNext();
        }
    }

    /**
     * After becoming the leader we need to take control over the consensus for
     * orphaned instances. This method activates retransmission of propose
     * messages for instances, which we already have in our logs (
     * {@link sendNextProposal} and {@link Propose} create a new instance)
     * 
     * @param instance instance we want to revoke
     */
    private void continueProposal(ConsensusInstance instance) {
        assert state == ProposerState.PREPARED;
        assert proposeRetransmitters.containsKey(instance.getId()) == false : "Different proposal for the same instance";

        // TODO: current implementation causes temporary window size violation.
        Message m = new Propose(instance);

        // Mark the instance as accepted locally
        instance.getAccepts().set(processDescriptor.localId);

        RetransmittedMessage msg = retransmitter.startTransmitting(m);
        proposeRetransmitters.put(instance.getId(), msg);
    }

    /**
     * As the process looses leadership, it must stop all message retransmission
     * - that is either prepare or propose messages.
     */
    public void stopProposer() {
        state = ProposerState.INACTIVE;
        synchronized (pendingProposals) {
            acceptNewBatches = false;
            pendingProposals.clear();
            pendingProposals.notify();
        }
        prepareRetransmitter.stop();
        retransmitter.stopAll();
        proposeRetransmitters.clear();
    }

    /**
     * After reception of majority accepts, we suppress propose messages.
     * 
     * @param instanceId no. of instance, for which we want to stop
     *            retransmission
     */
    public void stopPropose(int instanceId) {
        assert paxos.getDispatcher().amIInDispatcher();

        RetransmittedMessage r = proposeRetransmitters.remove(instanceId);
        if (r != null) {
            r.stop();
        }
    }

    /**
     * If retransmission to some process for certain instance is no longer
     * needed, we should stop it
     * 
     * @param instanceId no. of instance, for which we want to stop
     *            retransmission
     * @param destination number of the process in processes PID list
     */
    public void stopPropose(int instanceId, int destination) {
        assert proposeRetransmitters.containsKey(instanceId);
        assert paxos.getDispatcher().amIInDispatcher();

        proposeRetransmitters.get(instanceId).stop(destination);
    }

    public ClientBatchManager getClientBatchManager() {
        return cliBatchManager;
    }

    private final static Logger logger = Logger.getLogger(ProposerImpl.class.getCanonicalName());
}