package lsr.paxos.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lsr.paxos.core.Paxos;

/**
 * Represents the algorithm responsible for recovering the paxos from crash.
 */
public abstract class RecoveryAlgorithm {
    private List<RecoveryListener> listeners = new ArrayList<RecoveryListener>();

    /**
     * Adds a new listener called after recovery is finished.
     * 
     * @param listener - the listener to add
     */
    public void addRecoveryListener(RecoveryListener listener) {
        listeners.add(listener);
    }

    /**
     * Removes a previously registered listener. This listener will not be
     * notified about finished recovery.
     * 
     * @param listener - the listener to unregister
     */
    public void removeRecoveryListener(RecoveryListener listener) {
        listeners.remove(listener);
    }

    /**
     * Notifies all registered recovery listeners that the recovery has been
     * finished.
     */
    protected void fireRecoveryListener() {
        for (RecoveryListener listener : listeners) {
            listener.recoveryFinished();
        }
    }

    /**
     * Returns recovered paxos protocol. This method can be called before the
     * recovery is finished.
     * 
     * @return recovered paxos
     */
    public abstract Paxos getPaxos();

    /**
     * Starts the recovery algorithm.
     * 
     * @throws IOException if some I/O error occurs
     */
    public abstract void start() throws IOException;
}
