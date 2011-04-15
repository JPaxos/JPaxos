package lsr.paxos;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher.Priority;
import lsr.common.NoOperationRequest;
import lsr.common.ProcessDescriptor;
import lsr.common.Request;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.Prepare;
import lsr.paxos.messages.PrepareOK;
import lsr.paxos.messages.Propose;
import lsr.paxos.network.Network;
import lsr.paxos.replica.Replica.CrashModel;
import lsr.paxos.statistics.ReplicaStats;
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
class ProposerImpl implements Proposer {

    /** retransmitted message for prepare request */
    private PrepareRetransmitter prepareRetransmitter;

    /** retransmitted propose messages for instances */
    private final Map<Integer, RetransmittedMessage> proposeRetransmitters =
            new HashMap<Integer, RetransmittedMessage>();

    /** Keeps track of the processes that have prepared for this view */
    private final Retransmitter retransmitter;
    private final Paxos paxos;
    private final Storage storage;
    private final FailureDetector failureDetector;
    private final Network network;

    private final ArrayDeque<Request> pendingProposals = new ArrayDeque<Request>();
    private ProposerState state;

    private BatchBuilder batchBuilder;

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
    public ProposerImpl(Paxos paxos, Network network, FailureDetector failureDetector,
                        Storage storage, CrashModel crashModel) {
        this.paxos = paxos;
        this.network = network;
        this.failureDetector = failureDetector;
        this.storage = storage;
        this.retransmitter = new Retransmitter(this.network,
                ProcessDescriptor.getInstance().numReplicas,
                this.paxos.getDispatcher());

        // Start view 0. Process 0 assumes leadership without executing a
        // prepare round, since there's nothing to prepare
        this.state = ProposerState.INACTIVE;

        if (crashModel == CrashModel.EpochSS)
            prepareRetransmitter = new EpochPrepareRetransmitter(retransmitter, storage);
        else
            prepareRetransmitter = new PrepareRetransmitterImpl(retransmitter);
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
        assert state == ProposerState.INACTIVE : "Proposer is ACTIVE.";
        assert paxos.getDispatcher().amIInDispatcher();

        state = ProposerState.PREPARING;
        setNextViewNumber();
        failureDetector.leaderChange(paxos.getLeaderId());

        Prepare prepare = new Prepare(storage.getView(), storage.getFirstUncommitted());
        prepareRetransmitter.startTransmitting(prepare, storage.getAcceptors());

        logger.info("Preparing view: " + storage.getView());
    }

    private void setNextViewNumber() {
        int view = storage.getView();
        do {
            view++;
        } while (view % ProcessDescriptor.getInstance().numReplicas != ProcessDescriptor.getInstance().localId);
        storage.setView(view);
    }

    /**
     * 
     * @param message
     * @param sender
     * @throws InterruptedException
     */
    public void onPrepareOK(PrepareOK message, int sender) {
        assert paxos.getDispatcher().amIInDispatcher();
        assert paxos.isLeader();
        assert state != ProposerState.INACTIVE : "Proposer is not active.";
        // A process sends a PrepareOK message only as a response to a
        // Prepare message. Therefore, for this process to receive such
        // a message it must have sent a prepare message, so it must be
        // on a phase equal or higher than the phase of the prepareOk
        // message.
        assert message.getView() == storage.getView() : "Received a PrepareOK for a higher or lower view. " +
                                                        "Msg.view: " +
                                                        message.getView() +
                                                        ", view: " + storage.getView();

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
            stopPreparingStartProposing();
        }
    }

    private void stopPreparingStartProposing() {
        prepareRetransmitter.stop();
        state = ProposerState.PREPARED;

        logger.info("View prepared " + storage.getView());
        ReplicaStats.getInstance().advanceView(storage.getView());
                

        // Send a proposal for all instances that were not decided.
        Log log = storage.getLog();
        for (int i = storage.getFirstUncommitted(); i < log.getNextId(); i++) {
            ConsensusInstance instance = log.getInstance(i);
            // May happen if prepareOK caused a snapshot
            if (instance == null)
                continue;
            switch (instance.getState()) {
                case DECIDED:
                    // If the decision was already taken by some process,
                    // there is no need to propose again, so skip this
                    // instance
                    break;

                case KNOWN:
                    // No decision, but some process already accepted it.
                    logger.info("Proposing locked value: " + instance);
                    instance.setView(storage.getView());
                    continueProposal(instance);
                    break;

                case UNKNOWN:
                    assert instance.getValue() == null : "Unknow instance has value";
                    logger.info("No value locked for instance " + i + ": proposing no-op");
                    fillWithNoOperation(instance);
            }
        }

        batchBuilder = new BatchBuilder();
        // TODO: NS: Probably not needed as there is no proposal waiting
        // Shouldn't the leader send propose for unfinished instances?
        batchBuilder.enqueueRequests();
        
        // NS: Benchmark
//        ReplicaCommandCallback.instance.startBenchmark();
    }

    private void fillWithNoOperation(ConsensusInstance instance) {
        instance.setValue(storage.getView(), new NoOperationRequest().toByteArray());
        continueProposal(instance);
    }

    private void updateLogFromPrepareOk(PrepareOK message) {
        if (message.getPrepared() == null)
            return;
        // Update the local log with the data sent by this process
        for (int i = 0; i < message.getPrepared().length; i++) {
            ConsensusInstance ci = message.getPrepared()[i];
            // Algorithm: The received instance can be either
            // Decided - Set the local log entry to decided.
            // Accepted - If the local log entry is decided, ignore.
            // Otherwise, find the accept message for this consensus
            // instance with the highest timestamp and propose it.
            ConsensusInstance localLog = storage.getLog().getInstance(ci.getId());
            // Happens if previous PrepareOK caused a snapshot execution
            if (localLog == null)
                continue;
            if (localLog.getState() == LogEntryState.DECIDED) {
                // We already know the decision, so ignore it.
                continue;
            }
            switch (ci.getState()) {
                case DECIDED:
                    localLog.setValue(ci.getView(), ci.getValue());
                    paxos.decide(ci.getId());
                    break;

                case KNOWN:
                    localLog.setValue(ci.getView(), ci.getValue());
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

    /**
     * Asks the proposer to propose the given value. If there are currently too
     * many active propositions, this proposal will be enqueued until there are
     * available slots. If the proposer is <code>INACTIVE</code>, then message
     * is discarded. Otherwise value is added to list of active proposals.
     * 
     * @param value - the value to propose
     */
    public void propose(Request value) {
        assert paxos.getDispatcher().amIInDispatcher();

        if (state == ProposerState.INACTIVE) {
            logger.warning("Cannot propose on inactive state: " + value);
            return;
        }

        if (pendingProposals.contains(value)) {
            logger.warning("Value already queued for proposing. Ignoring: " + value);
            return;
        }

        pendingProposals.add(value);
        // If proposer is still preparing the view, the batch builder
        // is not created yet.
        if (state == ProposerState.PREPARED) {
            batchBuilder.enqueueRequests();
        }
    }

    /**
     * Called to inform the proposer that a decision was taken. Allows the
     * proposer to make a new proposal.
     */
    public void ballotFinished() {
        assert paxos.getDispatcher().amIInDispatcher();

        // There's a space on the window.
        // Try sending the current batch

        // During prepare phase the new leader might decide the
        // instances that were sent on PrepareOK messages. As
        // part of the normal decision process, it will call this
        // method. In this case, the batchBuilder is still null,
        // so this method shouldn't try to access it. It's also
        // not necessary, as the leader should not issue proposals
        // while preparing.
        if (state == ProposerState.PREPARED) {
            batchBuilder.enqueueRequests();
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
        proposeRetransmitters.put(instance.getId(),
                retransmitter.startTransmitting(m, storage.getAcceptors()));
    }

    /**
     * As the process looses leadership, it must stop all message retransmission
     * - that is either prepare or propose messages.
     */
    public void stopProposer() {
        state = ProposerState.INACTIVE;
        pendingProposals.clear();
        if (batchBuilder != null) {
            batchBuilder.cancel();
            batchBuilder = null;
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
        if (r != null)
            r.stop();
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

    final class BatchBuilder implements Runnable {
        /**
         * Holds the proposals that will be sent on the batch until the batch is
         * ready to be sent. By delaying serialization of all proposals until
         * the size of the batch is known, it's possible to create a byte[] for
         * the batch with the exact size, therefore avoiding the creation of a
         * temporary buffer.
         */
        final private ArrayList<Request> batchReqs = new ArrayList<Request>(16);
        /** The header takes 4 bytes */
        private int batchSize = 4;

        /*
         * If the batch is ready to be sent. This is true if either of the
         * following is true: - The size of the batch exceeds the maximum
         * allowed batch size (ProcessDescriptor.batchingLevel) This happens if
         * a single proposal is bigger than the max batch size. - Adding the
         * next pending proposal to the batch would exceed the max batch size -
         * The batch is non-empty and it was created more than
         * ProcessDescriptor.maxBatchDelay time ago
         */
        private boolean ready = false;
        /** If the batch was sent or canceled */
        private boolean cancelled = false;
        /** Builds the string with the log message */
        private final StringBuilder sb;

        public BatchBuilder() {
            // avoid creating object if log message is not going to be written
            /*
             * WARNING: during shutdown, the LogManager runs a shutdown hook
             * that resets all loggers, setting their level to null. The root
             * logger remains at INFO. Therefore, calls to isLoggable(level),
             * with level >= INFO might start returning true, even if the log
             * level was set to WARNING or higher. This behavior caused a NPE in
             * the code below. The sb was not initialized because when this
             * object is created the _logger.isLoggable(Level.INFO) returns
             * false, but is later accessed when isLoggable(Level.INFO) return
             * true. (during shutdown). To avoid this, we check if sb == null
             * before trying to access it.
             */
            if (logger.isLoggable(Level.INFO)) {
                sb = new StringBuilder(64);
                sb.append("Proposing: ").append(storage.getLog().getNextId());
            } else {
                sb = null;
            }
        }

        public void cancel() {
            this.cancelled = true;
        }

        /**
         * Tries to complete a batch by taking requests from _pendingProposals.
         * 
         */
        public void enqueueRequests() {

            if (ready) {
                trySend();
                return;
            }

            if (pendingProposals.isEmpty()) {
                logger.fine("enqueueRequests(): No proposal available.");
                return;
            }

            while (!pendingProposals.isEmpty()) {
                Request request = pendingProposals.getFirst();

                if (batchReqs.isEmpty()) {
                    int delay = ProcessDescriptor.getInstance().maxBatchDelay;
                    if (delay <= 0) {
                        // Do not schedule the task if delay is 0
                        ready = true;
                    } else {
                        // Schedule this task for execution within MAX_DELAY
                        // time
                        paxos.getDispatcher().schedule(this, Priority.High, delay);
                    }
                }

                /*
                 * If the batch is empty, add the request unconditionally. This
                 * is to handle requests bigger than the batchingLevel, we have
                 * to exceed the limit otherwise the request would not be
                 * ordered.
                 */
                if (batchSize + request.byteSize() <= ProcessDescriptor.getInstance().batchingLevel ||
                    batchReqs.isEmpty()) {
                    batchSize += request.byteSize();
                    batchReqs.add(request);
                    pendingProposals.removeFirst();
                    // See comment on constructor for sb!=null
                    if (sb != null && logger.isLoggable(Level.FINE)) {
                        sb.append(request.getRequestId().toString()).append(",");
                    }
                } else {
                    // no space for next request and batchReqs not empty. Send
                    // the batch
                    ready = true;
                    break;
                }
            }

            if (batchSize >= ProcessDescriptor.getInstance().batchingLevel) {
                ready = true;
            }

            trySend();
        }

        private void trySend() {
            int nextID = storage.getLog().getNextId();

            if (batchReqs.isEmpty() || !storage.isInWindow(nextID)) {
                // Nothing to send or window is full
                return;
            }

            // If no instance is running, always send
            // Otherwise, send if the batch is ready.
            if (ready) {
                send();
            }
        }

        public void run() {
            if (cancelled) {
                return;
            }

            // Deadline expired. Should not delay batch any further.
            ready = true;
            trySend();
        }

        private void send() {
            // Can send proposal
            ByteBuffer bb = ByteBuffer.allocate(batchSize);
            bb.putInt(batchReqs.size());
            for (Request req : batchReqs) {
                req.writeTo(bb);
            }
            byte[] value = bb.array();

            ConsensusInstance instance = storage.getLog().append(storage.getView(), value);

            assert proposeRetransmitters.containsKey(instance.getId()) == false : "Different proposal for the same instance";

            // See comment on constructor for sb!=null
            if (sb != null) {
                sb.append(" Size:").append(value.length);
                sb.append(", k=").append(batchReqs.size());
                logger.info(sb.toString());
            }

            if (ProcessDescriptor.getInstance().benchmarkRun) {
                int alpha = instance.getId() - storage.getFirstUncommitted() + 1;
                ReplicaStats.getInstance().consensusStart(instance.getId(), value.length,
                        batchReqs.size(), alpha);
            }

            // creating retransmitter, which automatically starts
            // sending propose message to all acceptors
            Message message = new Propose(instance);
            BitSet destinations = storage.getAcceptors();

            // Mark the instance as accepted locally
            instance.getAccepts().set(ProcessDescriptor.getInstance().localId);
            // Do not send propose message to self.
            destinations.clear(ProcessDescriptor.getInstance().localId);

            proposeRetransmitters.put(instance.getId(),
                    retransmitter.startTransmitting(message, destinations));

            // If this task was not yet executed by the dispatcher,
            // this flag ensures that the message is not sent twice.
            cancelled = true;
            // Proposer can start a new batch
            ProposerImpl.this.batchBuilder = new BatchBuilder();
        }
    }

    private final static Logger logger = Logger.getLogger(ProposerImpl.class.getCanonicalName());
}