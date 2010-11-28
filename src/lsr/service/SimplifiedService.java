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

    /**
     * Makes snapshot for current state of <code>Service</code>.
     * <p>
     * The same data created in this method, will be used to update state from
     * other snapshot using {@link #updateToSnapshot(byte[])} method.
     * 
     * @return the data containing current state
     */
    protected abstract byte[] makeSnapshot();

    /**
     * Updates the current state of <code>Service</code> to state from snapshot.
     * This method will be called after recovery to restore previous state, or
     * if we received new one from other replica (using catch-up).
     * 
     * @param snapshot - data used to update to new state
     */
    protected abstract void updateToSnapshot(byte[] snapshot);

    public final byte[] execute(byte[] value, int seqNo) {
        lastExecutedSeq = seqNo;
        return execute(value);
    }

    public final void askForSnapshot(int lastNextSeq) {
        forceSnapshot(lastNextSeq);
    }

    public final void forceSnapshot(int lastNestSeq) {
        byte[] snapshot = makeSnapshot();
        fireSnapshotMade(lastExecutedSeq + 1, snapshot, null);
    }

    public final void updateToSnapshot(int nextSeq, byte[] snapshot) {
        lastExecutedSeq = nextSeq - 1;
        updateToSnapshot(snapshot);
    }
}