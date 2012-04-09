package lsr.paxos;

import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.MovingAverage;
import lsr.common.ProcessDescriptor;
import lsr.paxos.replica.Replica;
import lsr.paxos.storage.LogListener;
import lsr.paxos.storage.Storage;

/**
 * This class is informed when the log size is changed, asking the state machine
 * (if necessary) for a snapshot.
 * 
 * If a snapshot is created by the state machine, SnapshotMaintainer writes it
 * to storage and truncates logs.
 */
public class SnapshotMaintainer implements LogListener {
	
	public static final int MAX_SIZE = 100;
	private final Replica replica;

    public SnapshotMaintainer(Replica replica) { 
		this.replica = replica;
	}

    /**
     * Decides if a snapshot needs to be requested based on the current size of
     * the log
     */
	@Override
    public void logSizeChanged(int newsize) {				
		if(newsize > MAX_SIZE){
			replica.executeDoSnapshot();
		}
    }

    private final static Logger logger = Logger.getLogger(SnapshotMaintainer.class.getCanonicalName());
}
