package lsr.paxos.core;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Configuration;
import lsr.common.MovingAverage;
import lsr.common.Pair;
import lsr.common.Range;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.CatchUpListener;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.CatchUpQuery;
import lsr.paxos.messages.CatchUpResponse;
import lsr.paxos.messages.CatchUpSnapshot;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.replica.ClientBatchManager;
import lsr.paxos.replica.ClientBatchManager.FwdBatchRetransmitter;
import lsr.paxos.storage.ClientBatchStore;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Storage;

public class CatchUp {

    private Storage storage;
    private SnapshotProvider snapshotProvider;
    private Network network;
    private Paxos paxos;

    private SingleThreadDispatcher paxosDispatcher;

    /** moving average factor used for changing timeout */
    private static final double convergenceFactor = 0.2;
    /** Initial, conservative value. Updated as a moving average. */
    private static final long INITIAL_RETRANSMIT_TIMEOUT = processDescriptor.retransmitTimeout;
    /** Can send queries no faster than... */
    private static final long MIN_CATCHUP_QUERY_RESEND_TIMEOUT_MS = 250;
    /** On no response, how often should catch up query be sent. */
    private MovingAverage resendTimeout = new MovingAverage(convergenceFactor,
            INITIAL_RETRANSMIT_TIMEOUT);

    private final Object switchingCatchUpTaskLock = new Object();
    private ScheduledFuture<?> catchUpTask = null;
    long lastQuerySent = 0;

    /**
     * Replica rating rules for catch-up:
     * <ol>
     * <li>each replica starts with rating 0
     * <li>as we send the request, we decrease the value by number of instances
     * requested
     * <li>as we receive the request, we increase the value by number of
     * instances acquired
     * <li>if the sendMessage function throws an exception, the rating is set to
     * negative value
     * <li>An empty, not periodic response reaches us, we decrees down by 5 the
     * value (but not under 0)
     * </ol>
     * 
     * Selecting best replica:
     * <ol>
     * <li>We check, if we were not asked to select the leader (happens if other
     * replica answered it has no value for us)
     * <li>We select replicas that is neither me not leader
     * <li>the replica with best rating is chosen
     * <li>if the best rating is negative, we're catching up with the leader,
     * and we reset all ratings; otherwise we catch up with the selected one
     * </ol>
     */

    /** holds replica rating for choosing best replica for catch-up */
    private int[] replicaRating;

    /** Holds all listeners that want to know about catch-up state change */
    HashSet<CatchUpListener> listeners = new HashSet<CatchUpListener>();

    public CatchUp(SnapshotProvider snapshotProvider, Paxos paxos, Storage storage, Network network) {
        this.snapshotProvider = snapshotProvider;
        this.network = network;
        this.paxosDispatcher = paxos.getDispatcher();
        MessageHandler handler = new InnerMessageHandler();
        Network.addMessageListener(MessageType.CatchUpQuery, handler);
        Network.addMessageListener(MessageType.CatchUpResponse, handler);
        Network.addMessageListener(MessageType.CatchUpSnapshot, handler);

        this.paxos = paxos;
        this.storage = storage;
        replicaRating = new int[processDescriptor.numReplicas];
    }

    /**
     * Forces the catch-up to send query
     */
    public void forceCatchup() {
        synchronized (switchingCatchUpTaskLock) {
            if (catchUpTask != null)
                // already catching up. Ignore.
                return;

            assert !paxos.isLeader();

            logger.info("Starting normal catchup");

            catchUpTask = paxosDispatcher.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    sendQuery();
                }
            }, 0, (long) resendTimeout.get(), TimeUnit.MILLISECONDS);
        }
    }

    private void finished() {
        synchronized (switchingCatchUpTaskLock) {
            if (catchUpTask == null)
                // normal catch-up is not working, ignore
                return;
            catchUpTask.cancel(false);
            catchUpTask = null;
        }
    }

    public void dispatchCatchUp(boolean immediatly) {
        synchronized (switchingCatchUpTaskLock) {
            if (catchUpTask == null) {
                logger.warning("//TODO: WHEN_CAN_THIS_HAPPEN?");
                return;
            }

            catchUpTask.cancel(false);

            long initialDelay = immediatly ? 0 : (long) resendTimeout.get();
            long now = System.currentTimeMillis();
            long timeToNextQuery = now + initialDelay;
            long nextAllowedTime = lastQuerySent + MIN_CATCHUP_QUERY_RESEND_TIMEOUT_MS;
            initialDelay = Math.max(timeToNextQuery, nextAllowedTime) - now;

            catchUpTask = paxosDispatcher.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    sendQuery();
                }
            },
                    initialDelay,
                    (long) resendTimeout.get(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Main catch-up process: we're creating (and later updating) the list of
     * undecided instances, and sending it.
     * 
     * We're trying to reach best replica possible, and as we get the needed
     * information, we exit.
     */
    private void sendQuery() {
        assert paxosDispatcher.amIInDispatcher() : "Must be running on the Protocol thread";
        /*
         * A follower may submit a catch-up task for execution and then become
         * leader before the task runs. As the leader never needs to catch-up
         * (the view change ensures that it becomes up-to-date), we ignore the
         * catch-up.
         */
        if (paxos.isLeader()) {
            logger.warning("Interrupting catchup. Replica is in leader role");
            finished();
            return;
        }

        int target = getBestContactReplica();
        assert target != processDescriptor.localId : "Selected self for catch-up";

        CatchUpQuery query = new CatchUpQuery(storage.getView(), new int[0], new Range[0]);
        int requestedInstanceCount = fillUnknownList(query);

        network.sendMessage(query, target);
        lastQuerySent = System.currentTimeMillis();

        // Modifying the rating of replica we're catching up with
        // We don't count the additional logSize+1 number requested

        replicaRating[target] -= requestedInstanceCount;

        logger.info("Sent " + query.toString() + " to [p" + target + "]");
    }

    /**
     * the predicate being true when the catch-up mechanism should finish
     */
    private boolean assumeSucceded() {
        // the current is OK for catch-up caused by the window violation, and
        // not for periodical one. Consider writing a method giving more sense
        return storage.isInWindow(storage.getLog().getNextId() - 1);
    }

    /**
     * If a replica responds that it has even not a single value for us, we're
     * trying to catch-up with the leader
     */
    private boolean askLeader = false;

    /**
     * Should return the ID to replica best suiting for catch-up; may change
     * during catching-up
     */
    private int getBestContactReplica() {
        // TODO: verify code changing replica ratings

        if (askLeader) {
            askLeader = false;
            return paxos.getLeaderId();
        }

        // BitSet candidates has all processes without his and the leader
        BitSet candidates = new BitSet(processDescriptor.numReplicas);
        candidates.set(0, processDescriptor.numReplicas);
        candidates.clear(processDescriptor.localId);
        candidates.clear(paxos.getLeaderId());

        // replica with greatest rating is used
        int i = candidates.nextSetBit(0);
        int bestReplica = i;

        for (; i >= 0; i = candidates.nextSetBit(i + 1)) {
            if (replicaRating[i] > replicaRating[bestReplica]) {
                bestReplica = i;
            }
        }

        // If a replica has negative rating, we catch-up with the leader
        if (replicaRating[bestReplica] < 0) {
            bestReplica = paxos.getLeaderId();

            // For all but leader the value is set to zero
            for (int j = 0; j < replicaRating.length; ++j) {
                if (j != bestReplica) {
                    replicaRating[j] = 0;
                }
            }

        }

        return bestReplica;
    }

    /**
     * Generates (ascending) list of instance numbers, which we consider for
     * undecided yet on basis of the log, and adds the next instance number on
     * the end of this list
     * 
     * @param query
     * @return count of instances embedded
     */
    private int fillUnknownList(CatchUpQuery query) {
        List<Integer> unknownList = new ArrayList<Integer>();
        List<Range> unknownRange = new ArrayList<Range>();

        SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();

        if (log.isEmpty()) {
            query.setInstanceIdList(Collections.singletonList(storage.getLog().getNextId()));
            return 0;
        }

        int begin = -1;
        boolean previous = false;
        int lastKey = log.lastKey();
        int count = 1;

        ConsensusInstance instance;
        for (int i = Math.max(storage.getFirstUncommitted(), log.firstKey()); i <= lastKey; ++i) {
            instance = log.get(i);

            if (instance == null) {
                continue;
            }

            if (!instance.isDecidable()) {
                count++;
                if (!previous) {
                    begin = i;
                    previous = true;
                }
            } else if (previous) {
                assert begin != -1 : "Problem in unknown list creation 1";
                if (begin == i - 1) {
                    unknownList.add(begin);
                } else {
                    unknownRange.add(new Range(begin, i - 1));
                }
                previous = false;
            }
        }

        if (previous) {
            assert begin != -1 : "Problem in unknown list creation 2";
            if (begin == lastKey) {
                unknownList.add(begin);
            } else {
                unknownRange.add(new Range(begin, lastKey));
            }
        }

        unknownList.add(lastKey + 1);

        query.setInstanceIdList(unknownList);
        query.setInstanceIdRangeList(unknownRange);

        return count;
    }

    private void handleSnapshot(CatchUpSnapshot msg, int sender) {
        Snapshot snapshot = msg.getSnapshot();

        if (logger.isLoggable(Level.INFO))
            logger.info("Catch-up snapshot from [p" + sender + "] : " + msg.toString());

        replicaRating[sender] = Math.max(replicaRating[sender], 5);

        snapshotProvider.handleSnapshot(snapshot);
    }

    /**
     * Procedure handling the catch-up response - if it's empty, it's dropped,
     * otherwise we're adding proper event for the dispatcher
     */
    private void handleResponse(CatchUpResponse response, int sender) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Catch-up from [p" + sender + "] : " + response.toString());
        }

        List<ConsensusInstance> logFragment = response.getDecided();

        if (logFragment.isEmpty()) {

            // decrease the rating of a replica, that has no value at all
            replicaRating[sender] = Math.max(0, replicaRating[sender] - 5);

            askLeader = true;
            return;
        }

        replicaRating[sender] += 2 * logFragment.size();

        long processingTime = System.currentTimeMillis() - response.getRequestTime();

        resendTimeout.add(3 * processingTime);
        if (resendTimeout.get() < Configuration.CATCHUP_MIN_RESEND_TIMEOUT)
            resendTimeout.reset(Configuration.CATCHUP_MIN_RESEND_TIMEOUT);

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Changing resend timeout for Catch-Up to " + resendTimeout);
        }

        handleCatchUpEvent(logFragment);
    }

    /**
     * Main procedure for the replica responding to query.
     * 
     * The replica checks if the requested instances are decided by it, and if
     * yes - it appends them to response.
     */
    private void handleQuery(CatchUpQuery query, int sender) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Got " + query.toString() + " from [p" + sender + "]");
        }

        SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();

        if (log.isEmpty()) {
            if (storage.getLastSnapshot() != null) {
                sendSnapshotResponse(query, sender);
            } else {
                logger.warning("Log empty, no snapshot, yet a catch-up query received?");
            }
            return;
        }

        assert query.getInstanceIdArray().length > 0;

        // check if the lowest single ID is available
        if (query.getInstanceIdArray()[0] < log.firstKey()) {
            // if no, send snapshot
            sendSnapshotResponse(query, sender);
            return;
        }

        Integer lastKey = log.lastKey();
        ResponseSender responseSender = new ResponseSender(query, sender);

        // Adding instances from the requested ranges
        pairs : for (Pair<Integer, Integer> range : query.getInstanceIdRangeArray()) {
            for (int i = range.key(); i <= range.value() && i <= lastKey; ++i) {
                if (i > lastKey) {
                    break pairs;
                }
                ConsensusInstance consensusInstance = log.get(i);

                if (consensusInstance == null) {
                    sendSnapshotResponse(query, sender);
                    return;
                }

                if (consensusInstance.getState() == LogEntryState.DECIDED) {
                    responseSender.add(consensusInstance);
                }
            }
        }

        // We check for requested id's
        for (int instanceId : query.getInstanceIdArray()) {
            if (instanceId > lastKey) {
                continue;
            }
            ConsensusInstance consensusInstance = log.get(instanceId);

            if (consensusInstance == null) {
                sendSnapshotResponse(query, sender);
                return;
            }

            if (consensusInstance.getState() == LogEntryState.DECIDED) {
                responseSender.add(consensusInstance);
            }
        }

        responseSender.finish();
    }

    private void sendSnapshotResponse(CatchUpQuery query, int sender) {
        Snapshot lastSnapshot = storage.getLastSnapshot();
        assert lastSnapshot != null;
        Message m = new CatchUpSnapshot(storage.getView(), query.getSentTime(), lastSnapshot);
        if (logger.isLoggable(Level.INFO))
            logger.info("Sending snapshot " + m + " to [p" + sender + "]");
        network.sendMessage(m, sender);

        CatchUpQuery newQuery = trimQuery(query, lastSnapshot.getNextInstanceId());

        handleQuery(newQuery, sender);
    }

    /**
     * To re-use code for sending response, this function trims catch-up query
     * below the just sent snapshot
     */
    private CatchUpQuery trimQuery(CatchUpQuery query, int nextInstanceId) {
        List<Integer> iil = query.getInstanceIdList();
        while (!iil.isEmpty() && iil.get(0) < nextInstanceId) {
            iil.remove(0);
        }

        List<Range> iirl = query.getInstanceIdRangeList();
        while (!iirl.isEmpty() && iirl.get(0).getValue() < nextInstanceId) {
            iirl.remove(0);
        }

        if (!iirl.isEmpty() && iirl.get(0).getKey() < nextInstanceId) {
            if (iirl.get(0).getValue() == nextInstanceId) {
                iil.add(0, nextInstanceId);
                iirl.remove(0);
            } else
                iirl.get(0).setKey(nextInstanceId);
        }

        int iilMax = iil.isEmpty() ? -1 : iil.get(iil.size() - 1);
        int iirlMax = iirl.isEmpty() ? -1 : iirl.get(iirl.size() - 1).getValue();
        int max = Math.max(Math.max(iilMax, iirlMax), nextInstanceId);

        if (max + 1 < storage.getFirstUncommitted() - 1) {
            iirl.add(new Range(max + 1, storage.getFirstUncommitted() - 1));
        } else if (max + 1 == storage.getFirstUncommitted() - 1) {
            iil.add(max + 1);
        }

        iil.add(storage.getFirstUncommitted());

        return new CatchUpQuery(query.getView(), iil, iirl);
    }

    /**
     * updates the storage and request deciding the caught-up instances
     */
    private void handleCatchUpEvent(List<ConsensusInstance> logFragment) {
        paxosDispatcher.checkInDispatcher();

        for (final ConsensusInstance newInstance : logFragment) {
            final ConsensusInstance localInstance = storage.getLog().getInstance(
                    newInstance.getId());

            // A snapshot && log truncate took place; must have been decided
            if (localInstance == null) {
                continue;
            }

            if (localInstance.getState() == LogEntryState.DECIDED) {
                continue;
            }

            if (!processDescriptor.indirectConsensus
                || ClientBatchStore.instance.hasAllBatches(newInstance.getClientBatchIds())) {
                localInstance.updateStateFromDecision(newInstance.getView(), newInstance.getValue());
                paxos.decide(localInstance.getId());
            } else {
                localInstance.setDecidable(true);
                final BatchFetchingTimeoutTask bftt = new BatchFetchingTimeoutTask(
                        localInstance.getId());
                final ScheduledFuture<?> sf = paxosDispatcher.schedule(bftt,
                        processDescriptor.maxBatchFetchingTimeoutMs,
                        TimeUnit.MILLISECONDS);
                FwdBatchRetransmitter fbr = ClientBatchStore.instance.getClientBatchManager().fetchMissingBatches(
                        newInstance.getClientBatchIds(), new ClientBatchManager.Hook() {
                            public void hook() {
                                paxosDispatcher.execute(new Runnable() {
                                    public void run() {
                                        localInstance.updateStateFromDecision(
                                                newInstance.getView(), newInstance.getValue());
                                        if (!LogEntryState.DECIDED.equals(localInstance.getState()))
                                            paxos.decide(localInstance.getId());
                                        paxosDispatcher.remove(bftt);
                                        sf.cancel(false);
                                        checkCatchupSucceded(false);
                                    }
                                });
                            }
                        }, true);
                bftt.setFwdBatchForwarder(fbr);
                localInstance.setFwdBatchForwarder(fbr);
            }
        }
    }

    /**
     * If fetching the batch contents failed, this method re-triggers normal
     * catch-up for the instance.
     */
    protected class BatchFetchingTimeoutTask implements Runnable {

        private final int instanceId;
        private FwdBatchRetransmitter fbr = null;

        public BatchFetchingTimeoutTask(int instanceId) {
            this.instanceId = instanceId;
        }

        void setFwdBatchForwarder(FwdBatchRetransmitter fbr) {
            this.fbr = fbr;
        }

        public void run() {
            assert fbr != null;
            ClientBatchStore.instance.getClientBatchManager().removeTask(fbr);
            ConsensusInstance ci = storage.getLog().getInstance(instanceId);
            if (ci != null)
                // snapshot protection
                ci.setDecidable(false);
            checkCatchupSucceded(true);
        }
    }

    /** Needed to be notified about messages for catch-up */
    private class InnerMessageHandler implements MessageHandler {
        public void onMessageReceived(final Message msg, final int sender) {
            paxosDispatcher.submit(new Runnable() {
                public void run() {
                    /*
                     * catch-up messages are not parsed anywhere else, so one
                     * must check here if these do not contain a more recent
                     * view
                     */
                    if (msg.getView() > storage.getView())
                        paxosDispatcher.execute(new Runnable() {
                            public void run() {
                                paxos.advanceView(msg.getView());
                            }
                        });
                    // Handle the message itself
                    switch (msg.getType()) {
                        case CatchUpResponse:
                            handleResponse((CatchUpResponse) msg, sender);
                            checkCatchupSucceded(((CatchUpResponse) msg).isLastPart());
                            break;
                        case CatchUpQuery:
                            handleQuery((CatchUpQuery) msg, sender);
                            break;
                        case CatchUpSnapshot:
                            handleSnapshot((CatchUpSnapshot) msg, sender);
                            checkCatchupSucceded(false);
                            break;
                        default:
                            assert false : "Unexpected message type: " + msg.getType();
                    }
                }
            });
        }

        public void onMessageSent(Message message, BitSet destinations) {
            // Empty
        }
    }

    private void checkCatchupSucceded(boolean restartImmediatlyIfNot) {
        for (CatchUpListener listener : listeners) {
            listener.catchUpAdvanced();
        }
        if (assumeSucceded()) {
            logger.info("Catch-up succeedd");
            finished();
        } else {
            dispatchCatchUp(restartImmediatlyIfNot);
        }
    }

    private final static long EMPTY_RESPONSE_SIZE = (new CatchUpResponse(0, 0,
            new ArrayList<ConsensusInstance>())).toByteArray().length;
    private static final int MAX_RESPONSE_SIZE = processDescriptor.maxUdpPacketSize;

    /**
     * Collects instances to be sent
     */
    private class ResponseSender {
        private final CatchUpQuery query;
        private final int sender;
        private long currentSize;

        /**
         * Contains instances matching requested id's that we have marked as
         * decided
         */
        private final List<ConsensusInstance> availableInstances;

        public ResponseSender(CatchUpQuery query, int sender) {
            this.query = query;
            this.sender = sender;
            availableInstances = new ArrayList<ConsensusInstance>();
            currentSize = EMPTY_RESPONSE_SIZE;
        }

        public void add(ConsensusInstance instance) {
            long instanceSize = instance.byteSize();

            if (currentSize + instanceSize > MAX_RESPONSE_SIZE) {
                sendAvailablePart();
                currentSize = EMPTY_RESPONSE_SIZE;
            }
            currentSize += instanceSize;
            availableInstances.add(instance);
        }

        public void finish() {
            // if nothing has been sent, we need to sent empty message.
            // if anything has been sent, it had LastPart=false set, so we
            // need to send a message with lastPart=true

            CatchUpResponse response = new CatchUpResponse(
                    storage.getView(),
                    query.getSentTime(),
                    availableInstances);

            network.sendMessage(response, sender);
        }

        private void sendAvailablePart() {
            CatchUpResponse response = new CatchUpResponse(
                    storage.getView(),
                    query.getSentTime(),
                    availableInstances);
            response.setLastPart(false);

            network.sendMessage(response, sender);

            availableInstances.clear();
        }
    }

    /** Adds the listener, returns if succeeded */
    public boolean addListener(CatchUpListener listener) {
        return listeners.add(listener);
    }

    /** Removes the listener, returns if succeeded */
    public boolean removeListener(CatchUpListener listener) {
        return listeners.remove(listener);
    }

    private final static Logger logger = Logger.getLogger(CatchUp.class.getCanonicalName());
}
