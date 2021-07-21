package lsr.paxos.replica.storage;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

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
        unpackUB = executeUB;
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
    public void setLastReplyForClient(int instance, long client, Reply reply) {
        lastReplyForClientDiff.put(client, reply);
        if (!repliesInInstance.containsKey(instance))
            repliesInInstance.put(instance, new LinkedList<Reply>());
        repliesInInstance.get(instance).add(reply);
    }

    @Override
    public Integer getLastReplySeqNoForClient(long client) {
        Reply reply = lastReplyForClientDiff.get(client);
        if (reply != null)
            return reply.getRequestId().getSeqNumber();
        return super.getLastReplySeqNoForClient(client);
    }

    @Override
    public Reply getLastReplyForClient(long client) {
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

        HashSet<Long> newReplies = new HashSet<Long>(lastReplyForClientDiff.size() * 4 / 3);

        SortedMap<Integer, List<Reply>> repliesMapFragment = repliesInInstance.headMap(instance,
                false);

        while (!repliesMapFragment.isEmpty()) {
            // got from last instance down to the first
            int i = repliesMapFragment.lastKey();
            List<Reply> replies = repliesMapFragment.remove(i);

            for (Reply reply : replies) {
                long clientId = reply.getRequestId().getClientId();
                if (newReplies.contains(clientId))
                    // if a higher instance had a reply for that client
                    continue;
                newReplies.add(clientId);

                super.setLastReplyForClient(i, clientId, reply);

                // remove from diff if possible
                int diffSn = lastReplyForClientDiff.get(clientId).getRequestId().getSeqNumber();
                assert diffSn >= reply.getRequestId().getSeqNumber();
                if (diffSn == reply.getRequestId().getSeqNumber())
                    // remove iff there is no more recent reply for that client
                    // in a more recent instance
                    lastReplyForClientDiff.remove(clientId);
            }
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
