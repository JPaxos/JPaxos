package lsr.paxos.replica.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.Reply;
import lsr.paxos.Snapshot;
import lsr.paxos.storage.ConsensusInstance;

public class InMemoryReplicaStorage implements ReplicaStorage {

    /** Next request to be executed. */
    private volatile int executeUB = 0;

    private int unpackUB = executeUB;

    /**
     * Temporary storage for the instances that finished and are not yet
     * executed.
     * 
     * Warning: multi-thread access
     */
    private final NavigableMap<Integer, ConsensusInstance> decidedWaitingExecution = new TreeMap<Integer, ConsensusInstance>();

    // // // // // // // // // // // // // // // // // // // //
    // Cached replies and past replies for snapshot creation //
    // // // // // // // // // // // // // // // // // // // //

    /**
     * For each client, keeps the sequence id of the last request executed from
     * the client.
     * 
     * This is accessed by the Selector threads, so it must be thread-safe
     */
    private final Map<Long, Reply> lastReplyForClient = new ConcurrentHashMap<Long, Reply>(8192,
            (float) 0.75, 8);

    /**
     * Caches replies for all requests executed in given instance.
     * 
     * Contains information from the previous snapshot up to now.
     */
    private Map<Integer, List<Reply>> repliesInInstance = new HashMap<Integer, List<Reply>>();

    /**
     * Value of {@link #lastReplyForClient} for the previous call of
     * getLastRepliesUptoInstance.
     */
    private final Map<Long, Reply> previousSnapshotLastReplyForClient = new HashMap<Long, Reply>();

    /** Instance of the previous call of getLastRepliesUptoInstance */
    private int previousSnapshotInstance = 0;

    /*
     * TODO: the lastReplyForClient map grows and is NEVER cleared!
     *
     * (It's size is the number of unique clients of JPaxos)
     * 
     * For theoretical correctness, it must stay so. In practical approach, give
     * me unbounded storage, limit the overall client count or simply let eat
     * some stale client requests.
     * 
     * Possible "bad" solution keeping correctness: record time stamp from
     * client, his request will only be valid for 5 minutes, after that time -
     * go away. If client resends it after 5 minutes, we ignore request. If
     * client changes the time stamp, it's already a new request, right? Client
     * with broken clocks will have bad luck.
     */

    public InMemoryReplicaStorage() {
    }

    @Override
    public int getExecuteUB() {
        return executeUB;
    }

    @Override
    public void setExecuteUB(int executeUB) {
        assert (executeUB >= this.executeUB);
        this.executeUB = executeUB;
        unpackUB = executeUB;
    }

    @Override
    public void incrementExecuteUB() {
        executeUB++;
    }

    @Override
    public int getUnpackUB() {
        return unpackUB;
    }

    @Override
    public void incrementUnpackUB() {
        unpackUB++;
    }

    @Override
    public void addDecidedWaitingExecution(Integer instanceId, ConsensusInstance ci) {
        synchronized (decidedWaitingExecution) {
            decidedWaitingExecution.put(instanceId, ci);
        }
    }

    @Override
    public ConsensusInstance getDecidedWaitingExecution(Integer instanceId) {
        synchronized (decidedWaitingExecution) {
            return decidedWaitingExecution.get(instanceId);
        }
    }

    @Override
    public void releaseDecidedWaitingExecutionUpTo(int instanceId) {
        synchronized (decidedWaitingExecution) {
            while (!decidedWaitingExecution.isEmpty() &&
                   decidedWaitingExecution.firstKey() < instanceId)
                decidedWaitingExecution.pollFirstEntry();
        }
    }

    @Override
    public void releaseDecidedWaitingExecution(int instanceId) {
        synchronized (decidedWaitingExecution) {
            decidedWaitingExecution.remove(instanceId);
        }
    }

    @Override
    public int decidedWaitingExecutionCount() {
        synchronized (decidedWaitingExecution) {
            return decidedWaitingExecution.size();
        }
    }

    @Override
    public void setLastReplyForClient(int instance, long client, Reply reply) {
        lastReplyForClient.put(client, reply);

        List<Reply> repl = repliesInInstance.get(instance);
        if (repl == null) {
            repl = new Vector<Reply>();
            repliesInInstance.put(instance, repl);
        }

        repl.add(reply);
    }

    @Override
    public Reply getLastReplyForClient(long client) {
        return lastReplyForClient.get(client);
    }

    @Override
    public Integer getLastReplySeqNoForClient(long client) {
        Reply reply = getLastReplyForClient(client);
        if (reply == null)
            return null;
        return reply.getRequestId().getSeqNumber();
    }

    @Override
    public Map<Long, Reply> getLastRepliesUptoInstance(int instance) {
        assert (instance >= previousSnapshotInstance);

        for (int it = previousSnapshotInstance; it < instance; it++) {
            List<Reply> replies = repliesInInstance.remove(it);
            if (replies == null)
                // Iff the instance is a no-op
                continue;
            for (Reply reply : replies) {
                previousSnapshotLastReplyForClient.put(reply.getRequestId().getClientId(), reply);
            }

        }
        previousSnapshotInstance = instance;
        return previousSnapshotLastReplyForClient;
    }

    @Override
    public List<Reply> getRepliesInInstance(int instanceId) {
        return repliesInInstance.get(instanceId);
    }

    @Override
    public void restoreFromSnapshot(Snapshot snapshot) {
        lastReplyForClient.clear();
        lastReplyForClient.putAll(snapshot.getLastReplyForClient());

        previousSnapshotLastReplyForClient.clear();
        previousSnapshotLastReplyForClient.putAll(snapshot.getLastReplyForClient());

        repliesInInstance.clear();

        executeUB = snapshot.getNextInstanceId();

        releaseDecidedWaitingExecutionUpTo(executeUB);

        previousSnapshotInstance = executeUB;
    }

    static final Logger logger = LoggerFactory.getLogger(InMemoryReplicaStorage.class);

}
