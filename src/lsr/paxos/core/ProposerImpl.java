package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import lsr.common.CrashModel;
import lsr.paxos.ActiveRetransmitter;
import lsr.paxos.EpochPrepareRetransmitter;
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
import lsr.paxos.replica.ClientBatchManager.FwdBatchRetransmitter;
import lsr.paxos.replica.ClientRequestManager;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Log;
import lsr.paxos.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private ProposerState state;

    private ClientBatchManager cliBatchManager;

    /** Locked on the array, modifies the int inside. */
    private final int[] waitingHooks = new int[] {0};
    private final ArrayList<ClientBatchManager.FwdBatchRetransmitter> waitingFBRs = new ArrayList<ClientBatchManager.FwdBatchRetransmitter>();

    /** Tasks to be executed once the proposer prepares */
    final HashSet<Task> tasksOnPrepared = new HashSet<Task>();

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
                        Storage storage,
                        CrashModel crashModel)
    {
        this.paxos = paxos;
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
        assert !processDescriptor.indirectConsensus || cliBatchManager != null;
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

        state = ProposerState.PREPARING;
        setNextViewNumber();

        logger.info(processDescriptor.logMark_Benchmark, "Preparing view: {}", storage.getView());

        Prepare prepare = new Prepare(storage.getView(), storage.getFirstUncommitted());
        prepareRetransmitter.startTransmitting(prepare, Network.OTHERS);

        if (processDescriptor.indirectConsensus)
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
                !ClientBatchStore.instance.hasAllBatches(instane.getClientBatchIds())) {
                waitingHooks[0]++;
                FwdBatchRetransmitter fbr = cliBatchManager.fetchMissingBatches(
                        instane.getClientBatchIds(),
                        new ClientBatchManager.Hook() {

                            public void hook() {
                                synchronized (waitingHooks) {
                                    if (Thread.interrupted())
                                        return;
                                    waitingHooks[0]--;
                                    waitingHooks.notifyAll();
                                }
                            }
                        }, true);
                waitingFBRs.add(fbr);
            }
        }
    }

    private void setNextViewNumber() {
        int view = storage.getView();
        do {
            view++;
        } while (!processDescriptor.isLocalProcessLeader(view));
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

        logger.info(processDescriptor.logMark_Benchmark, "Received {}: {}", sender, message);

        // Ignore prepareOK messages if we have finished preparing
        if (state == ProposerState.PREPARED) {
            logger.debug("View {} already prepared. Ignoring message.", storage.getView());
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

        logger.debug("Majority of PrepareOK gathered. Waiting for {} missing batch values",
                waitingHooks[0]);

        long timeout = System.currentTimeMillis() + processDescriptor.maxBatchFetchingTimeoutMs;

        // wait for all batch values to arrive
        synchronized (waitingHooks) {
            while (waitingHooks[0] > 0)
                try {
                    long timeLeft = timeout - System.currentTimeMillis();
                    if (timeLeft <= 0) {
                        logger.warn("Could not fetch batch values - restarting view change");
                        for (FwdBatchRetransmitter fbr : waitingFBRs)
                            cliBatchManager.removeTask(fbr);
                        waitingFBRs.clear();
                        waitingHooks[0] = 0;
                        prepareNextView();
                        return;
                    }
                    waitingHooks.wait(timeLeft);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
        }

        waitingFBRs.clear();

        state = ProposerState.PREPARED;

        logger.info(processDescriptor.logMark_Benchmark, "View prepared {}", storage.getView());

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
                    logger.info("Proposing value from previous view: {}", instance);
                    instance.setView(storage.getView());
                    continueProposal(instance);
                    break;

                case UNKNOWN:
                    assert instance.getValue() == null : "Unknow instance has value";
                    logger.warn("No value locked for instance {}: proposing no-op", i);
                    fillWithNoOperation(instance);
                    break;

                default:
                    assert false;
            }
        }

        paxos.onViewPrepared(log.getNextId());

        for (Task task : tasksOnPrepared) {
            task.onPrepared();
        }
        tasksOnPrepared.clear();

        if (processDescriptor.indirectConsensus)
            enqueueOrphanedBatches();

        proposeNext();
    }

    public void executeOnPrepared(final Task task) {
        assert state != ProposerState.INACTIVE;
        paxos.getDispatcher().execute(new Runnable() {
            public void run() {
                if (state == ProposerState.INACTIVE) {
                    task.onFailedToPrepare();
                    return;
                }

                if (state == ProposerState.PREPARED) {
                    task.onPrepared();
                    return;
                }

                tasksOnPrepared.add(task);
            }
        });
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
                    if (!processDescriptor.indirectConsensus
                        || ClientBatchStore.instance.hasAllBatches(ci.getClientBatchIds())) {
                        localLog.updateStateFromDecision(ci.getView(), ci.getValue());
                        paxos.decide(ci.getId());
                    } else {
                        waitingHooks[0]++;
                        ci.setDecidable(true);
                        FwdBatchRetransmitter fbr = cliBatchManager.fetchMissingBatches(
                                ci.getClientBatchIds(), new ClientBatchManager.Hook() {

                                    public void hook() {
                                        paxos.getDispatcher().executeAndWait(new Runnable() {
                                            public void run() {
                                                localLog.updateStateFromDecision(ci.getView(),
                                                        ci.getValue());
                                                if (!LogEntryState.DECIDED.equals(ci.getState()))
                                                    paxos.decide(ci.getId());
                                            }
                                        });
                                        synchronized (waitingHooks) {
                                            if (Thread.interrupted())
                                                return;
                                            waitingHooks[0]--;
                                            waitingHooks.notifyAll();
                                        }
                                    }
                                }, true);
                        waitingFBRs.add(fbr);
                    }
                    break;

                case KNOWN:
                    assert ci.getValue() != null : "Instance state KNOWN but value is null";
                    if (!processDescriptor.indirectConsensus ||
                        ClientBatchStore.instance.hasAllBatches(ci.getClientBatchIds()))
                        localLog.updateStateFromKnown(ci.getView(), ci.getValue());
                    else {
                        waitingHooks[0]++;
                        FwdBatchRetransmitter fbr = cliBatchManager.fetchMissingBatches(
                                ci.getClientBatchIds(),
                                new ClientBatchManager.Hook() {

                                    public void hook() {
                                        paxos.getDispatcher().executeAndWait(new Runnable() {
                                            public void run() {
                                                localLog.updateStateFromKnown(ci.getView(),
                                                        ci.getValue());
                                            }
                                        });
                                        synchronized (waitingHooks) {
                                            if (Thread.interrupted())
                                                return;
                                            waitingHooks[0]--;
                                            waitingHooks.notifyAll();
                                        }
                                    }
                                }, true);
                        waitingFBRs.add(fbr);
                    }
                    break;

                case UNKNOWN:
                    assert ci.getValue() == null : "Unknow instance has value";
                    logger.debug("Ignoring: {}", ci);
                    break;

                default:
                    assert false : "Invalid state: " + ci.getState();
                    break;
            }
        }

    }

    public void notifyAboutNewBatch()
    {
        // Called from batcher thread
        paxos.getDispatcher().submit(new Runnable() {
            public void run() {
                logger.debug("Propose task running");
                proposeNext();
            }
        });
    }

    public void proposeNext() {
        logger.debug("Proposing.");
        while (!storage.isWindowFull()) {
            byte[] proposal = paxos.requestBatch();
            if (proposal == null)
                return;

            propose(proposal);
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
            logger.warn("Cannot propose in INACTIVE or PREPARING state. Discarding batch");
            return;
        }

        logger.info(processDescriptor.logMark_Benchmark, "Proposing: {}",
                storage.getLog().getNextId());

        ConsensusInstance instance = storage.getLog().append(storage.getView(), value);

        assert !processDescriptor.indirectConsensus ||
               ClientBatchStore.instance.hasAllBatches(instance.getClientBatchIds());

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
        assert paxos.getDispatcher().amIInDispatcher();
        state = ProposerState.INACTIVE;
        // TODO: STOP ACCEPTING
        prepareRetransmitter.stop();
        retransmitter.stopAll();
        proposeRetransmitters.clear();
        for (Task task : tasksOnPrepared) {
            task.onFailedToPrepare();
        }
        tasksOnPrepared.clear();
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

    private final static Logger logger = LoggerFactory.getLogger(ProposerImpl.class);
}