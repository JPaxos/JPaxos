package lsr.paxos.replica.storage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import lsr.common.Pair;
import lsr.common.Reply;
import lsr.paxos.Snapshot;
import lsr.paxos.replica.SnapshotListener2;
import lsr.paxos.storage.ConsensusInstance;

public class SnapshotlyPersistentReplicaStorage extends PersistentReplicaStorage
        implements SnapshotListener2 {

    volatile int maxSeenDecidedId;

    @Override
    public void onSnapshotMade(Snapshot snapshot) {
        int realExecuteUb = getExecuteUB();
        super.setExecuteUB(snapshot.getNextInstanceId());
        setExecuteUB(realExecuteUb);
        releaseDecidedWaitingExecutionUpTo(snapshot.getNextInstanceId());

        // this is approximate, but that's fine for the intended use
        maxSeenDecidedId = realExecuteUb + super.decidedWaitingExecutionCount();
    }

    @Override
    public void setExecuteUB(int executeUB) {
        executeUBCache = executeUB;
    }

    @Override
    public void incrementExecuteUB() {
        executeUBCache++;
    }

    @Override
    public void addDecidedWaitingExecution(Integer instanceId, ConsensusInstance ci) {
        super.addDecidedWaitingExecution(instanceId, ci);
        if (instanceId > maxSeenDecidedId)
            maxSeenDecidedId = instanceId;
    }

    @Override
    public int decidedWaitingExecutionCount() {
        return maxSeenDecidedId - executeUBCache;
    }

    @Override
    public void releaseDecidedWaitingExecution(int instanceId) {
        return;
        /* release is intentionally called at snapshot only */
    }

    private final NavigableMap<Integer, List<Reply>> repliesInInstance = new TreeMap<Integer, List<Reply>>();
    private final Map<Long, Reply> lastReplyForClientDiff = new ConcurrentHashMap<Long, Reply>();

    @Override
    public void setLastReplyForClient(int instance, Long client, Reply reply) {
        lastReplyForClientDiff.put(client, reply);
        if (!repliesInInstance.containsKey(instance))
            repliesInInstance.put(instance, new LinkedList<Reply>());
        repliesInInstance.get(instance).add(reply);
    }

    @Override
    public Integer getLastReplySeqNoForClient(Long client) {
        Reply reply = lastReplyForClientDiff.get(client);
        if (reply != null)
            return reply.getRequestId().getSeqNumber();
        return super.getLastReplySeqNoForClient(client);
    }

    @Override
    public Reply getLastReplyForClient(Long client) {
        Reply reply = lastReplyForClientDiff.get(client);
        if (reply != null)
            return reply;
        return super.getLastReplyForClient(client);
    }

    /* this is called only upon snapshot */
    @Override
    public Map<Long, Reply> getLastRepliesUptoInstance(int instance) {
        /*-
        // works, but is slow:
        while (!repliesInInstance.isEmpty() && repliesInInstance.firstKey() < instance) {
            Entry<Integer, List<Reply>> entry = repliesInInstance.pollFirstEntry();
            List<Reply> replies = entry.getValue();
            for (Reply reply : replies) {
                super.setLastReplyForClient(entry.getKey(), reply.getRequestId().getClientId(),
                        reply);
            }
        }
        return super.getLastRepliesUptoInstance(instance);
        -*/

        HashMap<Long, Pair<Integer, Reply>> newReplies = new HashMap<Long, Pair<Integer, Reply>>(
                lastReplyForClientDiff.size() * 4 / 3);

        SortedMap<Integer, List<Reply>> repliesMapFragment = repliesInInstance.headMap(instance);

        while (!repliesMapFragment.isEmpty()) {
            int i = repliesMapFragment.lastKey();
            List<Reply> replies = repliesMapFragment.remove(i);
            for (Reply reply : replies) {
                if (newReplies.containsKey(reply.getRequestId().getClientId()))
                    continue;
                newReplies.put(reply.getRequestId().getClientId(),
                        new Pair<Integer, Reply>(i, reply));
            }
        }

        for (Pair<Integer, Reply> e : newReplies.values()) {
            long clientId = e.getValue().getRequestId().getClientId();
            super.setLastReplyForClient(e.getKey(), clientId,
                    e.getValue());
            lastReplyForClientDiff.remove(clientId);
        }

        return super.getLastRepliesUptoInstance(instance);
    }

    @Override
    public List<Reply> getRepliesInInstance(int instanceId) {
        return repliesInInstance.get(instanceId);
    }

    @Override
    public void restoreFromSnapshot(Snapshot snapshot) {
        repliesInInstance.clear();
        lastReplyForClientDiff.clear();
        super.restoreFromSnapshot(snapshot);
    }
}
