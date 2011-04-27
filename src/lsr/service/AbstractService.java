package lsr.service;

import java.util.ArrayList;
import java.util.List;

import lsr.paxos.replica.SnapshotListener;

/**
 * Abstract class which can be used to simplify creating new services. It adds
 * implementation for handling snapshot listeners.
 */
public abstract class AbstractService implements Service {
    /** Listeners which will be notified about new snapshot made by service */
    protected List<SnapshotListener> listeners = new ArrayList<SnapshotListener>();

    public final void addSnapshotListener(SnapshotListener listener) {
        listeners.add(listener);
    }

    public final void removeSnapshotListener(SnapshotListener listener) {
        listeners.remove(listener);
    }

    /**
     * Notifies all active listeners that new snapshot has been made.
     * 
     * @param nextRequestSeqNo - the next sequential number (last executed
     *            sequential number+1) of created snapshot
     * @param snapshot - the data containing snapshot
     * @param response - if the snapshot is called within execute method for
     *            after the just executed request, the response must be provided
     */
    protected void fireSnapshotMade(int nextRequestSeqNo, byte[] snapshot, byte[] response) {
        for (SnapshotListener listener : listeners) {
            listener.onSnapshotMade(nextRequestSeqNo, snapshot, response);
        }
    }

    /**
     * Informs the service that the recovery process has been finished, i.e.
     * that the service is at least at the state later than by crashing.
     * 
     * Please notice, for some crash-recovery approaches this can mean that the
     * service is a lot further than by crash.
     * 
     * For many applications this has no real meaning.
     */
    public void recoveryFinished() {
    }

}