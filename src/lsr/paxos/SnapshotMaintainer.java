package lsr.paxos;

import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.MovingAverage;
import lsr.common.ProcessDescriptor;
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

    private final Storage storage;

    /** Current snapshot size estimate */
    private MovingAverage snapshotByteSizeEstimate = new MovingAverage(0.75,
            ProcessDescriptor.getInstance().firstSnapshotSizeEstimate);

    /**
     * After how many new instances we are recalculating if snapshot is needed.
     * By default it's 1/5 of instances for last snapshot.
     */
    private int samplingRate = ProcessDescriptor.getInstance().minSnapshotSampling;

    /** Instance, by which we calculated last time if we need snapshot */
    private int lastSamplingInstance = 0;

    private final Dispatcher dispatcher;
    private final SnapshotProvider snapshotProvider;

    /** Indicates if we asked for snapshot */
    private boolean askedForSnapshot = false;

    /** if we forced for snapshot */
    private boolean forcedSnapshot = false;

    public SnapshotMaintainer(Storage storage, Dispatcher dispatcher, SnapshotProvider replica) {
        this.storage = storage;
        this.dispatcher = dispatcher;
        this.snapshotProvider = replica;
    }

    /** Receives a snapshot from state machine, records it and truncates the log */
    public void onSnapshotMade(final Snapshot snapshot) {
        // Called by the Replica thread. Queue it for execution on the Paxos
        // dispatcher.
        dispatcher.dispatch(new Runnable() {
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
        });
    }

    /**
     * Decides if a snapshot needs to be requested based on the current size of
     * the log
     */
    public void logSizeChanged(int newsize) {
        assert dispatcher.amIInDispatcher() : "Only Dispatcher thread allowed. Called from " +
                                              Thread.currentThread().getName();

        // TODO: Fix snapshotting.
        // For the time being, disabled snapshotting for benchmarking
        if (ProcessDescriptor.getInstance().benchmarkRun) {
            // NS: Workaround to bug with snapshotting.
            if (newsize > 1000) {
                int nextID = storage.getLog().getNextId();
                storage.getLog().truncateBelow(Math.max(0, nextID - 500));
            }
            return;
        }

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
        }
    }

    private final static Logger logger = Logger.getLogger(SnapshotMaintainer.class.getCanonicalName());
}
