package lsr.paxos.replica.storage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lsr.common.Reply;
import lsr.paxos.Snapshot;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.PersistentConsensusInstance;

public class PersistentReplicaStorage implements ReplicaStorage {

    private static native int getExecuteUB_();

    volatile int executeUBCache = getExecuteUB_();

    int unpackUB = getExecuteUB_();

    @Override
    public int getExecuteUB() {
        return executeUBCache;
    }

    private static native void setExecuteUB_(int executeUB);

    @Override
    public void setExecuteUB(int executeUB) {
        setExecuteUB_(executeUB);
        executeUBCache = executeUB;
        unpackUB = executeUB;
    }

    private static native void incrementExecuteUB_();

    @Override
    public void incrementExecuteUB() {
        incrementExecuteUB_();
        executeUBCache++;
    }

    @Override
    public int getUnpackUB() {
        return unpackUB;
    }

    @Override
    public void incrementUnpackUB() {
        unpackUB++;
    }

    private static native void addDecidedWaitingExecution(int instanceId);

    @Override
    public void addDecidedWaitingExecution(Integer instanceId, ConsensusInstance ci) {
        addDecidedWaitingExecution(instanceId);
    }

    private static native boolean isDecidedWaitingExecution(int instanceId);

    @Override
    public ConsensusInstance getDecidedWaitingExecution(Integer instanceId) {
        if (!isDecidedWaitingExecution(instanceId))
            return null;
        return new PersistentConsensusInstance(instanceId);
    }

    @Override
    public native void releaseDecidedWaitingExecutionUpTo(int instanceId);

    @Override
    public native void releaseDecidedWaitingExecution(int instanceId);

    @Override
    public native int decidedWaitingExecutionCount();

    private static native void setLastReplyForClient(int instance, long client, long clientSeqNo,
                                                     byte[] reply);

    @Override
    public void setLastReplyForClient(int instance, long client, Reply reply) {
        setLastReplyForClient(instance, client, reply.getRequestId().getSeqNumber(),
                reply.getValue());

    }

    /** Returns -1 if there was no reply */
    private static native int getLastReplySeqNoForClient_(long client);

    @Override
    public Integer getLastReplySeqNoForClient(long client) {
        int seqNo = getLastReplySeqNoForClient_(client);
        return seqNo == -1 ? null : seqNo;
    }

    private static native Reply getLastReplyForClient_(long client);

    @Override
    public Reply getLastReplyForClient(long client) {
        return getLastReplyForClient_(client);
    }

    private static native Reply[] getLastRepliesUptoInstance_(int instance);

    @Override
    public Map<Long, Reply> getLastRepliesUptoInstance(int instance) {
        HashMap<Long, Reply> map = new HashMap<Long, Reply>();
        for (Reply reply : getLastRepliesUptoInstance_(instance))
            map.put(reply.getRequestId().getClientId(), reply);
        return map;
    }

    private static native Reply[] getRepliesInInstance_(int instanceId);

    @Override
    public List<Reply> getRepliesInInstance(int instanceId) {
        Reply[] repliesInInstance = getRepliesInInstance_(instanceId);
        if (repliesInInstance == null)
            return null;
        return Arrays.asList(repliesInInstance);
    }

    private static native void dropLastReplyForClient();

    private static native void dropRepliesInInstance();

    @Override
    public void restoreFromSnapshot(Snapshot snapshot) {
        dropLastReplyForClient();
        dropRepliesInInstance();

        for (Reply reply : snapshot.getLastReplyForClient().values()) {
            int lastFullyExecutedInstance = snapshot.getNextInstanceId() - 1;
            long clientId = reply.getRequestId().getClientId();
            int clientSeqNo = reply.getRequestId().getSeqNumber();
            // FIXME: This below works, but is potentially __very__ slow.
            // Profile perf it.
            setLastReplyForClient(lastFullyExecutedInstance, clientId, clientSeqNo,
                    reply.getValue());
        }

        /*- TODO: this is also called by DecideCallbackImpl.atRestoringStateFromSnapshot - investigate -*/
        setExecuteUB(snapshot.getNextInstanceId());

        releaseDecidedWaitingExecutionUpTo(snapshot.getNextInstanceId());
    }

}
