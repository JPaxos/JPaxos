package lsr.paxos.replica.storage;

import java.util.List;
import java.util.Map;

import lsr.common.Reply;
import lsr.paxos.Snapshot;
import lsr.paxos.storage.ConsensusInstance;

/**
 * Class containing data that must not be lost upon crash/recovery for
 * correctness of the state machine replication
 */

public interface ReplicaStorage {

    /** Instance number such that all lower are already executed */
    int getExecuteUB();

    /** @see #getExecuteUB() */
    void setExecuteUB(int executeUB);

    /** @see #getExecuteUB() */
    void incrementExecuteUB();

    /** Decided instances wait here for execution */
    void addDecidedWaitingExecution(Integer instanceId, ConsensusInstance ci);

    /** Decided instances are taken for execution */
    ConsensusInstance getDecidedWaitingExecution(Integer instanceId);

    /** Executed instances in range [0, instanceId) are thrown away */
    void releaseDecidedWaitingExecutionUpTo(int instanceId);

    /** Executed instance instanceId are thrown away */
    void releaseDecidedWaitingExecution(int instanceId);

    /**
     * Returns the number of decided instances that wait for being executed by
     * the Service
     */
    int decidedWaitingExecutionCount();

    /**
     * Stores the reply for the client; used in case the reply is lost and
     * client sends the request again, and in case when a duplicated stale
     * request arrives
     * 
     * called by Replica thread
     */
    void setLastReplyForClient(int instance, Long client, Reply reply);

    /**
     * @see #setLastReplyForClient(int, int, Reply)
     *
     *      called by multiple threads (at least Replica, ClientManager)
     */
    Integer getLastReplySeqNoForClient(Long client);

    /**
     * @see #setLastReplyForClient(int, int, Reply)
     * 
     *      called by multiple threads (at least Replica, ClientManager)
     */
    Reply getLastReplyForClient(Long client);

    /**
     * Used upon creating snapshot. Extracts collection of last replies for each
     * client just before executing the given instance
     * 
     * <b>Warning:</b> subsequent calls to this function are expected to be
     * called with increasing {@code instance} parameter, and to release
     * resources of answering calls with lower {@code instance} parameter.
     */
    Map<Long, Reply> getLastRepliesUptoInstance(int instance);

    /**
     * Returns all replies previously added by
     * {@link #setLastReplyForClient(int, Long, Reply)} for given instance,
     * provided {@code instanceId} is higher than the argument of lat call of
     * {@link #getLastRepliesUptoInstance(int)}
     */
    List<Reply> getRepliesInInstance(int instanceId);

    /**
     * Called upon restoring state from snapshot in order to bring the
     * ReplicaStorage to the state from snapshot
     */
    void restoreFromSnapshot(Snapshot snapshot);
}
