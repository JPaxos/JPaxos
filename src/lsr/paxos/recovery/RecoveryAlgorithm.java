package lsr.paxos.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lsr.paxos.Paxos;

public abstract class RecoveryAlgorithm {
    private List<RecoveryListener> listeners = new ArrayList<RecoveryListener>();

    public void addRecoveryListener(RecoveryListener listener) {
        listeners.add(listener);
    }

    public void removeRecoveryListener(RecoveryListener listener) {
        listeners.remove(listener);
    }

    protected void fireRecoveryListener() {
        for (RecoveryListener listener : listeners) {
            listener.recoveryFinished();
        }
    }

    // must be available
    public abstract Paxos getPaxos();

    public abstract void start() throws IOException;
}
