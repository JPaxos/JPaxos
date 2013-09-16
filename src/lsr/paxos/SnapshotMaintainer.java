package lsr.paxos;

import static lsr.common.ProcessDescriptor.processDescriptor;
import lsr.common.MovingAverage;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.storage.LogListener;
import lsr.paxos.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            processDescriptor.firstSnapshotSizeEstimate);

    /**
     * After how many new instances we are recalculating if snapshot is needed.
     * By default it's 1/5 of instances for last snapshot.
     */
    private int samplingRate = processDescriptor.minSnapshotSampling;

    /** Instance, by which we calculated last time if we need snapshot */
    private int lastSamplingInstance = 0;

    private final SingleThreadDispatcher paxosDispatcher;
    private final SnapshotProvider snapshotProvider;

    /** Indicates if we asked for snapshot */
    private boolean askedForSnapshot = false;

    /** if we forced for snapshot */
    private boolean forcedSnapshot = false;

    public SnapshotMaintainer(Storage storage, SingleThreadDispatcher dispatcher,
                              SnapshotProvider replica) {
        this.storage = storage;
        this.paxosDispatcher = dispatcher;
        this.snapshotProvider = replica;
    }

    /** Receives a snapshot from state machine, records it and truncates the log */
    public void onSnapshotMade(final Snapshot snapshot) {
        // Called by the Replica thread. Queue it for execution on the Paxos
        // dispatcher.
        paxosDispatcher.submit(new Runnable() {
            public void run() {

                logger.debug("Snapshot made. next instance: {}", snapshot.getNextInstanceId());

                int previousSnapshotInstanceId = 0;

                Snapshot lastSnapshot = storage.getLastSnapshot();
                if (lastSnapshot != null) {
                    previousSnapshotInstanceId = lastSnapshot.getNextInstanceId();

                    if (previousSnapshotInstanceId > snapshot.getNextInstanceId()) {
                        logger.warn("Got snapshot older than current one! Dropping.");
                        return;
                    }
                }

                storage.setLastSnapshot(snapshot);

                storage.getLog().truncateBelow(previousSnapshotInstanceId);
                askedForSnapshot = forcedSnapshot = false;
                snapshotByteSizeEstimate.add(snapshot.getValue().length);

                if (logger.isDebugEnabled(processDescriptor.logMark_OldBenchmark))
                    logger.debug(
                            processDescriptor.logMark_OldBenchmark,
                            "Snapshot received from state machine for: {} (previous: {}) New size estimate: {}",
                            snapshot.getNextInstanceId(), previousSnapshotInstanceId,
                            snapshotByteSizeEstimate.get());

                samplingRate = Math.max(
                        (snapshot.getNextInstanceId() - previousSnapshotInstanceId) / 5,
                        processDescriptor.minSnapshotSampling);
            }
        });
    }

    /**
     * Decides if a snapshot needs to be requested based on the current size of
     * the log
     */
    public void logSizeChanged(int newsize) {
        assert paxosDispatcher.amIInDispatcher() : "Only Dispatcher thread allowed. Called from " +
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

        logger.debug("Calculated log size for {}", logByteSize);

        // Don't do a snapshot if the log is too small
        if (logByteSize < processDescriptor.snapshotMinLogSize) {
            return;
        }

        double sizeRatio = logByteSize / snapshotByteSizeEstimate.get();

        if (!askedForSnapshot) {
            if (sizeRatio < processDescriptor.snapshotAskRatio) {
                return;
            }

            logger.debug("Asking state machine for shapshot");

            snapshotProvider.askForSnapshot();
            askedForSnapshot = true;
            return;
        }

        if (!forcedSnapshot) {
            if (sizeRatio < processDescriptor.snapshotForceRatio) {
                return;
            }

            logger.debug("Forcing state machine to do shapshot");

            snapshotProvider.forceSnapshot();
            forcedSnapshot = true;
            return;
        }

    }

    private final static Logger logger = LoggerFactory.getLogger(SnapshotMaintainer.class);
}
