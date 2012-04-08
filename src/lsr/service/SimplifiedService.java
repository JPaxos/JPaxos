package lsr.service;

import lsr.paxos.replica.Replica;

/**
 * This class provides skeletal implementation of {@link Service} interface to
 * simplify creating services. To create new service using this class programmer
 * needs to implement following methods:
 * <ul>
 * <li><code>execute</code></li>
 * <li><code>makeSnapshot</code></li>
 * <li><code>updateToSnapshot</code></li>
 * </ul>
 * <p>
 * In most cases this methods will provide enough functionality. Creating
 * snapshots is invoked by framework. If more control for making snapshot is
 * needed then <code>Service</code> interface should be implemented.
 * <p>
 * All methods are called from the same thread, so it is not necessary to
 * synchronize them.
 * 
 */
public abstract class SimplifiedService extends AbstractService {
    private int lastExecutedSeq;

    /**
     * Executes one command from client on this state machine. This method will
     * be called by {@link Replica}.
     * 
     * @param value - value of instance to execute on this service
     * @return generated reply which will be sent to client
     */
    protected abstract byte[] execute(byte[] value);
	
	public final byte[] execute(byte[] value, int seqNo) {
        lastExecutedSeq = seqNo;
        return execute(value);
    }
}