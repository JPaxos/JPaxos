package lsr.paxos.replica.storage;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import lsr.common.Pair;
import lsr.common.Reply;
import lsr.paxos.Snapshot;

public class InMemoryServiceProxyStorage implements ServiceProxyStorage {

    /**
     * Sorted list of request sequence number starting each consensus instance.
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
    public LinkedList<Pair<Integer, Integer>> startingSeqNo = new LinkedList<Pair<Integer, Integer>>();

    /** The sequence number of next request passed to service. */
    private int nextSeqNo = 0;

    /** The sequence number of first request executed after last snapshot. */
    private int lastSnapshotNextSeqNo = -1;

    /**
     * Describes how many requests on should be skipped. Used only after
     * updating from snapshot.
     */
    private int skip = 0;

    /**
     * Holds responses for skipped requests. Used only after updating from
     * snapshot.
     */
    private Queue<Reply> skippedCache;

    public InMemoryServiceProxyStorage() {
        addStartingSeqenceNo(0, 0);
    }

    @Override
    public int getNextSeqNo() {
        return nextSeqNo;
    }

    @Override
    public void setNextSeqNo(int seqNo) {
        nextSeqNo = seqNo;
    }

    @Override
    public void incNextSeqNo() {
        nextSeqNo++;
    }

    @Override
    public void setSkip(int skip) {
        this.skip = skip;
    }

    @Override
    public boolean decSkip() {
        if (skip == 0)
            return false;
        skip--;
        return true;
    }

    @Override
    public boolean hasSkip() {
        return skip != 0;
    }

    @Override
    public byte[] getNextSkippedRequestResponse() {
        assert !skippedCache.isEmpty();
        return skippedCache.poll().getValue();
    }

    @Override
    public int getLastSnapshotNextSeqNo() {
        return lastSnapshotNextSeqNo;
    }

    @Override
    public void setLastSnapshotNextSeqNo(int lssn) {
        lastSnapshotNextSeqNo = lssn;
    }

    @Override
    public void setSkippedCache(List<Reply> partialResponseCache) {
        skippedCache = new LinkedList<Reply>(partialResponseCache);
    }

    @Override
    public void addStartingSeqenceNo(int instance, int sequenceNo) {
        startingSeqNo.add(new Pair<Integer, Integer>(instance, sequenceNo));
    }

    @Override
    public void truncateStartingSeqNo(int lowestSeqNo) {
        Pair<Integer, Integer> previous = null;
        while (!startingSeqNo.isEmpty() && startingSeqNo.getFirst().getValue() <= lowestSeqNo) {
            previous = startingSeqNo.pollFirst();
        }

        if (previous != null) {
            startingSeqNo.addFirst(previous);
        }
    }

    @Override
    public void clearStartingSeqNo() {
        startingSeqNo.clear();
    }

    @Override
    public Pair<Integer, Integer> getFrontStartingSeqNo() {
        return startingSeqNo.getFirst();
    }

    @Override
    public void onSnapshotMade(Snapshot snapshot) {
    }
}
