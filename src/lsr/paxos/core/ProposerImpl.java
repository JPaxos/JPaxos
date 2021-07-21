package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.CrashModel;
import lsr.paxos.ActiveRetransmitter;
import lsr.paxos.EpochPrepareRetransmitter;
import lsr.paxos.PrepareRetransmitter;
import lsr.paxos.PrepareRetransmitterImpl;
import lsr.paxos.RetransmittedMessage;
import lsr.paxos.UnBatcher;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientBatchManager;
import lsr.paxos.replica.ClientBatchManager.FwdBatchRetransmitter;
import lsr.paxos.replica.storage.ReplicaStorage;
import lsr.paxos.replica.ClientRequestBatcher;
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
    private final Map<Integer, RetransmittedMessage> proposeRetransmitters = new HashMap<Integer, RetransmittedMessage>();

    /** Keeps track of the processes that have prepared for this view */
    private final ActiveRetransmitter retransmitter;
    private final Paxos paxos;
    private final Storage storage;
    private final ReplicaStorage replicaStorage;

    private ClientBatchManager cliBatchManager;
    private ClientRequestBatcher cliRequestBatcher;

    /** Locked on the array, modifies the int inside. */
    private final int[] waitingHooks = new int[] {0};
    private final ArrayList<ClientBatchManager.FwdBatchRetransmitter> waitingFBRs = new ArrayList<ClientBatchManager.FwdBatchRetransmitter>();

    /** Tasks to be executed once the proposer prepares */
    final HashSet<OnLeaderElectionResultTask> tasksOnPrepared = new HashSet<OnLeaderElectionResultTask>();

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
                        ReplicaStorage replicaStorage,
                        CrashModel crashModel) {
        this.paxos = paxos;
        this.storage = storage;
        this.replicaStorage = replicaStorage;
        retransmitter = new ActiveRetransmitter(network, "ProposerRetransmitter");

        if (crashModel == CrashModel.EpochSS) {
            prepareRetransmitter = new EpochPrepareRetransmitter(retransmitter, storage);
        } else {
            prepareRetransmitter = new PrepareRetransmitterImpl(retransmitter);
        }
    }

    public void setClientRequestManager(ClientRequestManager requestManager) {
        cliBatchManager = requestManager.getClientBatchManager();
        cliRequestBatcher = requestManager.getClientRequestBatcher();
    }

    public void start() {
        assert !processDescriptor.indirectConsensus || cliBatchManager != null;
        retransmitter.init();
    }

    /**
     * Gets the current state of the proposer.
     * 
     * @return <code>INACTIVE</code>, <code>PREPARING<code/> or
     *         <code>PREPARED<code/>
     */
    public ProposerState getState() {
        return storage.getProposerState();
    }

    private void setState(ProposerState state) {
        storage.setProposerState(state);
    }

    /**
     * If previous leader is suspected this procedure is executed. We're
     * changing the view (variable indicating order of the leaders in time)
     * accordingly, and we're sending the prepare message.
     * 
     */
    public void prepareNextView() {
        assert paxos.getDispatcher().amIInDispatcher();

        setState(ProposerState.PREPARING);
        setNextViewNumber();
        paxos.getBatcher().suspendBatcher();

        startPreparingThisView();
    }

    private void startPreparingThisView() {

        logger.info(processDescriptor.logMark_Benchmark, "Preparing view: {}", storage.getView());

        OnLeaderElectionResultTask batcherTask = paxos.getBatcher().preparingNewView();
        if (batcherTask != null)
            executeOnPrepared(batcherTask);

        Prepare prepare = new Prepare(storage.getView(), storage.getFirstUncommitted());
        prepareRetransmitter.startTransmitting(prepare, Network.OTHERS);

        if (logger.isInfoEnabled(processDescriptor.logMark_Benchmark2019))
            logger.info(processDescriptor.logMark_Benchmark2019, "P1A S {}", prepare.getView());

        if (processDescriptor.indirectConsensus)
            fetchLocalMissingBatches();

        // tell that local process is already prepared
        prepareRetransmitter.update(null, processDescriptor.localId);
        // unlikely, unless N==1
        if (prepareRetransmitter.isMajority()) {
            onMajorityOfPrepareOK();
        }
    }

    /**
     * When a leader crashes & recovers in a crash model that allows to continue
     * being the leader (e.g. pmem), then this method is called.
     */
    public void continueAfterRecovery() {
        logger.info(processDescriptor.logMark_Benchmark, "Old leader restarting in view: {}",
                storage.getView());
        if (getState() == ProposerState.PREPARING) {
            startPreparingThisView();
        } else {
            assert (getState() == ProposerState.PREPARED);
            doPrepareThisView();
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
        assert getState() != ProposerState.INACTIVE : "Proposer is not active.";

        // asserting the same again. Who knows what happens in between?
        assert message.getView() == storage.getView() : "Received a PrepareOK for a higher or lower view. " +
                                                        "Msg.view: " + message.getView() +
                                                        ", view: " + storage.getView();

        if (logger.isDebugEnabled(processDescriptor.logMark_Benchmark2019))
            logger.debug(processDescriptor.logMark_Benchmark2019, "P1B R {} {}", message.getView(),
                    sender);

        logger.info(processDescriptor.logMark_Benchmark, "Received {}: {}", sender, message);

        // Ignore prepareOK messages if we have finished preparing
        if (getState() == ProposerState.PREPARED) {
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

        doPrepareThisView();
    }

    /**
     * Called when all PrepareOK and all batch values arrived
     */
    private void doPrepareThisView() {
        setState(ProposerState.PREPARED);

        if (logger.isDebugEnabled(processDescriptor.logMark_Benchmark2019))
            logger.debug(processDescriptor.logMark_Benchmark2019, "PREP {}", storage.getView());

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
                    instance.updateStateFromPropose(processDescriptor.localId, storage.getView(),
                            instance.getValue());
                    continueProposal(instance);
                    break;
                case RESET:
                    // as above, but we missed some proposal and got an accept,
                    // but no one in a majority knows a value
                    logger.info("Proposing value from previous view: {}", instance);
                    instance.updateStateFromPropose(processDescriptor.localId, storage.getView(),
                            instance.getValue());
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

        for (OnLeaderElectionResultTask task : tasksOnPrepared) {
            task.onPrepared();
        }
        tasksOnPrepared.clear();

        if (processDescriptor.indirectConsensus)
            enqueueOrphanedBatches();

        proposeNext();
    }

    public void executeOnPrepared(final OnLeaderElectionResultTask task) {
        assert getState() != ProposerState.INACTIVE;
        paxos.getDispatcher().submit(new Runnable() {
            public void run() {
                if (getState() == ProposerState.INACTIVE) {
                    task.onFailedToPrepare();
                    return;
                }

                if (getState() == ProposerState.PREPARED) {
                    task.onPrepared();
                    return;
                }

                tasksOnPrepared.add(task);
            }
        });
    }

    private void enqueueOrphanedBatches() {
        final HashSet<ClientBatchID> instanceless = ClientBatchStore.instance.getInstancelessBatches();
        new Thread() {
            public void run() {
                try {
                    for (ClientBatchID cbid : instanceless)
                        paxos.enqueueRequest(cbid, cliRequestBatcher);
                } catch (InterruptedException e) {
                    throw new RuntimeException(
                            "Interrupted while attepting to enqueue orphaned batches");
                }
            };
        }.start();
    }

    private void fillWithNoOperation(ConsensusInstance instance) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(0); // Size of batch
        instance.updateStateFromPropose(processDescriptor.localId, storage.getView(), bb.array());
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
                    if (!processDescriptor.indirectConsensus ||
                        ClientBatchStore.instance.hasAllBatches(ci.getClientBatchIds())) {
                        localLog.updateStateFromDecision(ci.getLastSeenView(),
                                ci.getValue());
                        paxos.decide(ci.getId());
                    } else {
                        waitingHooks[0]++;
                        ci.setDecidable(true);
                        FwdBatchRetransmitter fbr = cliBatchManager.fetchMissingBatches(
                                ci.getClientBatchIds(), new ClientBatchManager.Hook() {

                                    public void hook() {
                                        paxos.getDispatcher().executeAndWait(new Runnable() {
                                            public void run() {
                                                localLog.updateStateFromDecision(
                                                        ci.getLastSeenView(),
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
                        localLog.updateStateFromPropose(processDescriptor.localId,
                                ci.getLastSeenView(), ci.getValue());
                    else {
                        waitingHooks[0]++;
                        FwdBatchRetransmitter fbr = cliBatchManager.fetchMissingBatches(
                                ci.getClientBatchIds(),
                                new ClientBatchManager.Hook() {

                                    public void hook() {
                                        paxos.getDispatcher().executeAndWait(new Runnable() {
                                            public void run() {
                                                localLog.updateStateFromPropose(
                                                        processDescriptor.localId,
                                                        ci.getLastSeenView(),
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

                case RESET:
                    /* ~ ~ ~ fall through ~ ~ ~ */
                default:
                    assert false : "Invalid state: " + ci.getState();
                    break;
            }
        }

    }

    public void notifyAboutNewBatch() {
        // Called from batcher thread
        paxos.getDispatcher().submit(new Runnable() {
            public void run() {
                logger.debug("Propose task running");
                proposeNext();
            }
        });
    }

    volatile AtomicBoolean overdecisionInhibitedProposal = new AtomicBoolean(false);

    @Override
    public void instanceExecuted(int instanceId) {
        if (overdecisionInhibitedProposal.getAndSet(false)) {
            paxos.getDispatcher().submit(new Runnable() {
                public void run() {
                    if (getState() == ProposerState.PREPARED) {
                        logger.debug("An instance executed, attempting to propose");
                        proposeNext();
                    }
                }
            });
        }
    }

    public void proposeNext() {
        logger.debug("Proposing.");
        while (true) {
            if (storage.isWindowFull()) {
                logger.trace("Window full - not proposing");
                return;
            }

            int executeUB = replicaStorage.getExecuteUB();
            int nextId = storage.getLog().getNextId();

            if ((nextId - executeUB) > processDescriptor.decidedButNotExecutedThreshold) {
                if (logger.isDebugEnabled())
                    logger.debug(
                            "Too many decided but not executed - not poposing (next: {}/executeUb: {}/thres: {})",
                            storage.getLog().getNextId(), replicaStorage.getExecuteUB(),
                            processDescriptor.decidedButNotExecutedThreshold);
                overdecisionInhibitedProposal.set(true);
                if (executeUB != replicaStorage.getExecuteUB())
                    continue;
                return;
            }

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
        if (getState() != ProposerState.PREPARED) {
            /*
             * This can happen if there is a Propose event queued on the
             * Dispatcher when the view changes.
             */
            logger.warn("Cannot propose in INACTIVE or PREPARING state. Discarding batch");
            return;
        }

        logger.info(processDescriptor.logMark_OldBenchmark, "Proposing: {}",
                storage.getLog().getNextId());

        ConsensusInstance instance = storage.getLog().append();

        Propose proposeMsg = new Propose(storage.getView(), instance.getId(), value);

        boolean isMajority = instance.updateStateFromPropose(processDescriptor.localId,
                proposeMsg.getView(), proposeMsg.getValue());

        assert !processDescriptor.indirectConsensus ||
               ClientBatchStore.instance.hasAllBatches(instance.getClientBatchIds());

        if (isMajority) {
            logger.warn("Either you use one replica only (what for?) or something is very wrong.");
            paxos.decide(instance.getId());
        }

        RetransmittedMessage msg = retransmitter.startTransmitting(proposeMsg);
        proposeRetransmitters.put(instance.getId(), msg);

        if (logger.isTraceEnabled(processDescriptor.logMark_Benchmark2019nope))
            logger.trace(processDescriptor.logMark_Benchmark2019nope, "IP {} {}",
                    proposeMsg.getInstanceId(), UnBatcher.countCR(proposeMsg.getValue()));

    }

    /**
     * Called to inform the proposer that a decision was taken. Allows the
     * proposer to make a new proposal.
     */
    public void ballotFinished() {
        assert paxos.getDispatcher().amIInDispatcher();

        // Needed - decide (triggering this method) is i.a. called by PrepareOK
        if (getState() == ProposerState.PREPARED) {
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
        assert getState() == ProposerState.PREPARED;
        assert proposeRetransmitters.containsKey(
                instance.getId()) == false : "Different proposal for the same instance";

        // TODO: current implementation causes temporary window size violation.
        Propose m = new Propose(instance);

        boolean isMajority = instance.isMajority();

        // first send propose, then check if we're done with instance, or else
        // retransmitter ∞'s
        RetransmittedMessage msg = retransmitter.startTransmitting(m);
        proposeRetransmitters.put(instance.getId(), msg);

        if (logger.isTraceEnabled(processDescriptor.logMark_Benchmark2019nope))
            logger.trace(processDescriptor.logMark_Benchmark2019nope, "IP {} {}", m.getInstanceId(),
                    UnBatcher.countCR(m.getValue()));

        if (isMajority) {
            // Possible if PrepareOK messages carried enough votes to decide an
            // instance
            logger.warn(
                    "Called continueProposal for an instance with majority of acks. Sending propose and deciding immediatly.");
            paxos.decide(instance.getId());
        }
    }

    /**
     * As the process looses leadership, it must stop all message retransmission
     * - that is either prepare or propose messages.
     */
    public void stopProposer() {
        assert paxos.getDispatcher().amIInDispatcher();
        setState(ProposerState.INACTIVE);
        // TODO: STOP ACCEPTING
        prepareRetransmitter.stop();
        retransmitter.stopAll();
        proposeRetransmitters.clear();
        for (OnLeaderElectionResultTask task : tasksOnPrepared) {
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
