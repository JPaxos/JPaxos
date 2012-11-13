package lsr.paxos;

import java.util.Deque;

import lsr.paxos.replica.ClientBatchID;

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
     * @param requests - decided requests
     */
    void onRequestOrdered(int instance, Deque<ClientBatchID> requests);

}
