package lsr.paxos;

import java.util.Deque;

import lsr.common.ClientBatch;

/**
 * Batches the requests - packs multiple requests into one value for deciding
 * 
 * @author Jan K
 */
public interface Batcher {
    /** Transforms previously packed requests back to the queue */
    public Deque<ClientBatch> unpack(byte[] source);

}
