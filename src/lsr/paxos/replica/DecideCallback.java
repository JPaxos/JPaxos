package lsr.paxos.replica;

import lsr.paxos.storage.ConsensusInstance;

/**
 * This interface should be implemented by classes which want to be notified,
 * about new decided consensus instance.
 */
public interface DecideCallback {

    /**
     * Callback called every time new instance value has been decided. When
     * using batching, each instance of consensus may decide on more than one
     * requests. In that case, the <code>values</code> list will contain the
     * requests decided by the order that they should be executed.
     * 
     * @param instance - the id of instance which was decided
     * @param ci - decided requests
     */
    void onRequestOrdered(int instance, ConsensusInstance ci);

    /**
     * Upon recovering from the persistent memory some instances can be decided
     * and not yet executed, and there might be be no onRequestOrdered call.
     * Thus, this method is called to start the execution of such instances.
     */
    void scheduleExecuteRequests();

    /**
     * At restoring from snapshot the decide callback has to know what instance
     * to produce next. This is how it learns it.
     * 
     * @param nextInstanceId - next instance to be executed
     */
    void atRestoringStateFromSnapshot(int nextInstanceId);

    /**
     * Returns if the queue of requests decided, but not yet executed is large
     * enough to wait longer for new client requests in the batcher module
     */
    boolean hasDecidedNotExecutedOverflow();

}
