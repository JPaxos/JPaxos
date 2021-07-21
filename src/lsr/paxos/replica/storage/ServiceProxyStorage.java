package lsr.paxos.replica.storage;

import java.util.List;

import lsr.common.Pair;
import lsr.common.Reply;
import lsr.paxos.Snapshot;

public interface ServiceProxyStorage {

    public void incNextSeqNo();

    public int getNextSeqNo();

    public void setNextSeqNo(int seqNo);

    /**
     * Upon recovering from a snapshot, it may happen that part of the batch in
     * a given instance is executed; the number of already executed requests in
     * the batch is the 'skip'. As long as decSkip()==true, execute() will not
     * execute the request.
     */
    public void setSkip(int skip);

    /**
     * See {@link ServiceProxyStorage.setSkip}.
     * 
     * @return true iff next request has to be skipped
     */
    public boolean decSkip();

    /**
     * See {@link ServiceProxyStorage.setSkip}.
     * 
     * @return response for the next skipped request
     */
    public byte[] getNextSkippedRequestResponse();

    public int getLastSnapshotNextSeqNo();

    public void setLastSnapshotNextSeqNo(int lssn);

    public void setSkippedCache(List<Reply> partialResponseCache);

    /**
     * Records the first sequence number of a request in the given instance
     */
    public void addStartingSeqenceNo(int instance, int sequenceNo);

    /**
     * startingSeqNo is a sorted list of request sequence number starting each
     * consensus instance.
     * <p>
     * Example. Assume we executed following consensus instances:
     * 
     * <pre>
     * ConsensusInstance 0
     *   Request 0
     *   Request 1
     * ConsensusInstance 1
     *   Request 2
     *   Request 3
     *   Request 4
     * ConsensusInstance 2
     *   Request 5
     * </pre>
     * 
     * Then this list will contain following pairs:
     * 
     * <pre>
     * [0, 0]
     * [1, 2]
     * [2, 5]
     * </pre>
     * 
     * The sequence number of first request in consensus instance 2 is 5, etc.
     */

    /**
     * Truncates the startingSeqNo list so that value of first pair on the list
     * will be less or equal than specified <code>lowestSeqNo</code> and value
     * of second pair will be greater than <code>lowestSeqNo</code>. In other
     * words, key of first pair will equal to id of consensus instance that
     * contains request with sequence number <code>lowestSeqNo</code>.
     * <p>
     * Example: Given startingSeqNo containing:
     * 
     * <pre>
     * [0, 0]
     * [1, 5]
     * [2, 10]
     * [3, 15]
     * [4, 20]
     * </pre> After truncating to request 12, startingSeqNo will contain:
     * 
     * <pre>
     * [2, 10]
     * [3, 15]
     * [4, 20]
     * </pre>
     * 
     * <pre>
     * 10 <= 12 < 15
     * </pre>
     * 
     * @param lowestSeqNo
     */
    public void truncateStartingSeqNo(int lowestSeqNo);

    public void clearStartingSeqNo();

    /** Lowest currently available entry in startingSeqNo */
    public Pair<Integer, Integer> getFrontStartingSeqNo();

    /**
     * Extra actions that this Storage implementation wants to do when Service
     * creates a new snapshot
     */
    public void onSnapshotMade(Snapshot snapshot);

    public boolean hasSkip();

}
