package lsr.paxos;

import java.util.Deque;

import lsr.common.Request;

/**
 * Batches the requests - packs multiple requests into one value for deciding
 * 
 * @author Jan K
 */
public interface Batcher {
    /** Transforms previously packed requests back to the queue */
    public Deque<Request> unpack(byte[] source);

}
