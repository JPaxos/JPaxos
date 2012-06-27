package lsr.paxos;

import java.util.Deque;

import lsr.common.ClientRequest;

/**
 * Batches the requests - packs multiple requests into one value for deciding
 * 
 * @author Jan K
 */
public interface BatchUnpacker {
    /** Transforms previously packed requests back to the queue */
    public Deque<ClientRequest> unpack(byte[] source);

}
