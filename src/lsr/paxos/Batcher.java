package lsr.paxos;

import java.util.Deque;
import java.util.logging.Logger;

import lsr.common.Request;

/**
 * Batches the requests - packs multiple requests into one value for deciding
 * 
 * @author Jan K
 */
public interface Batcher {

    /**
     * Makes a single byte array from requests, removing packed requests from
     * source
     */
    public byte[] pack(Deque<Request> source, StringBuilder sb, Logger logger);

    /** Transforms previously packed requests back to the queue */
    public Deque<Request> unpack(byte[] source);

}
