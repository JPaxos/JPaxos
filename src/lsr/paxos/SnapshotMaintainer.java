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
	private Replica replica;

    public SnapshotMaintainer(Replica replica) {
		this.replica = replica;
    }

    /** Receives a snapshot from state machine, records it and truncates the log */
    public void onSnapshotMade(final Snapshot snapshot) {
        // Called by the Replica thread. Queue it for execution on the Paxos
        // dispatcher.
      /*  dispatcher.dispatch(new Runnable() {
            public void run() {

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Snapshot made. next instance: " + snapshot.getNextInstanceId() +
                                ", log: " + storage.getLog().size());
                }

                int previousSnapshotInstanceId = 0;

                Snapshot lastSnapshot = storage.getLastSnapshot();
                if (lastSnapshot != null) {
                    previousSnapshotInstanceId = lastSnapshot.getNextInstanceId();

                    if (previousSnapshotInstanceId > snapshot.getNextInstanceId()) {
                        logger.warning("Got snapshot older than current one! Dropping.");
                        return;
                    }
                }

                storage.setLastSnapshot(snapshot);

                storage.getLog().truncateBelow(previousSnapshotInstanceId);
                askedForSnapshot = forcedSnapshot = false;
                snapshotByteSizeEstimate.add(snapshot.getValue().length);

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Snapshot received from state machine for:" +
                                snapshot.getNextInstanceId() + "(previous: " +
                                previousSnapshotInstanceId + ") New size estimate: " +
                                snapshotByteSizeEstimate.get());
                }

                samplingRate = Math.max(
                        (snapshot.getNextInstanceId() - previousSnapshotInstanceId) / 5,
                        ProcessDescriptor.getInstance().minSnapshotSampling);
            }
        });*/
    }

    /**
     * Decides if a snapshot needs to be requested based on the current size of
     * the log
     */
	@Override
    public void logSizeChanged(int newsize) {
				
		if(newsize > MAX_SIZE){
			replica.doSnapshot();
			/*dispatcher.execute(new Runnable() {
				public void run() {
					replica.doSnapshot();
				}
			});*/
		}
		
     /*   assert dispatcher.amIInDispatcher() : "Only Dispatcher thread allowed. Called from " +
                                              Thread.currentThread().getName();

        if (askedForSnapshot && forcedSnapshot) {
            return;
        }

        if ((storage.getLog().getNextId() - lastSamplingInstance) < samplingRate) {
            return;
        }

        lastSamplingInstance = storage.getLog().getNextId();
        Snapshot lastSnapshot = storage.getLastSnapshot();
        int lastSnapshotInstance = lastSnapshot == null ? 0 : lastSnapshot.getNextInstanceId();

        long logByteSize = storage.getLog().byteSizeBetween(lastSnapshotInstance,
                storage.getFirstUncommitted());

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Calculated log size for " + logByteSize);
        }

        // Don't do a snapshot if the log is too small
        if (logByteSize < ProcessDescriptor.getInstance().snapshotMinLogSize) {
            return;
        }

        if (!askedForSnapshot) {
            if ((logByteSize / snapshotByteSizeEstimate.get()) < ProcessDescriptor.getInstance().snapshotAskRatio) {
                return;
            }

            logger.fine("Asking state machine for shapshot");

            snapshotProvider.askForSnapshot();
            askedForSnapshot = true;
            return;
        }

        // NUNO: Don't ever force snapshots.
        // JK: why? The service may just ignore it if it wants so.
        // It's just a second info for the service
        if (!forcedSnapshot) {
            if ((logByteSize / snapshotByteSizeEstimate.get()) < ProcessDescriptor.getInstance().snapshotForceRatio) {
                return;
            }

            logger.fine("Forcing state machine to do shapshot");

            snapshotProvider.forceSnapshot();
            forcedSnapshot = true;
            return;
        }*/
    }

    //private final static Logger logger = Logger.getLogger(SnapshotMaintainer.class.getCanonicalName());
}
