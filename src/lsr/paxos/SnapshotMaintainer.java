package lsr.paxos;

import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Config;
import lsr.common.Dispatcher;
import lsr.common.MovingAverage;
import lsr.common.ProcessDescriptor;
import lsr.paxos.storage.LogListener;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

/**
 * This class is informed when the log size is changed, asking the state machine
 * (if necessary) for a snapshot.
 * 
 * If a snapshot is created by the state machine, SnapshotMaintainer writes it
 * to storage and truncates logs.
 */
public class SnapshotMaintainer implements LogListener {

	private final StableStorage _stableStorage;
	private final Storage _storage;

	/** Current snapshot size estimate */
	private MovingAverage _snapshotByteSizeEstimate = new MovingAverage(0.75, Config.firstSnapshotSizeEstimate);

	/**
	 * After how many new instances we are recalculating if snapshot is needed.
	 * By default it's 1/5 of instances for last snapshot.
	 */
	private int _samplingRate = 50;

	/** Instance, by which we calculated last time if we need snapshot */
	private int _lastSamplingInstance = 0;

	private final Dispatcher _dispatcher;
	private final SnapshotProvider _snapshotProvider;

	/** Indicates if we asked for snapshot */
	private boolean _askedForSnapshot = false;

	/** if we forced for snapshot */
	private boolean _forcedSnapshot = false;

	public SnapshotMaintainer(Storage storage, Dispatcher dispatcher, SnapshotProvider replica) {
		_storage = storage;
		_dispatcher = dispatcher;
		_snapshotProvider = replica;
		_stableStorage = storage.getStableStorage();
	}

	/** Receives a snapshot from state machine, records it and truncates the log */
	public void onSnapshotMade(final Snapshot snapshot) {
		// Called by the Replica thread. Queue it for execution on the Paxos
		// dispatcher.
		_dispatcher.dispatch(new Runnable() {
			public void run() {

				if (logger.isLoggable(Level.FINE)) {
					logger.fine("Snapshot made. Request: " + snapshot.requestSeqNo + ", log: "
							+ _stableStorage.getLog().size());
				}

				int previousSnapshotRequestSeqNo = 0;
				int previousSnapshotInstanceId = 0;

				Snapshot lastSnapshot = _stableStorage.getLastSnapshot();
				if (lastSnapshot != null) {
					previousSnapshotRequestSeqNo = lastSnapshot.requestSeqNo;
					previousSnapshotInstanceId = lastSnapshot.enclosingIntanceId;

					if (previousSnapshotRequestSeqNo > snapshot.requestSeqNo) {
						logger.warning("Got snapshot older than current one! Dropping.");
						return;
					}
				}

				_stableStorage.setLastSnapshot(snapshot);

				_stableStorage.getLog().truncateBelow(previousSnapshotInstanceId);
				_askedForSnapshot = _forcedSnapshot = false;
				_snapshotByteSizeEstimate.add(snapshot.value.length);

				if (logger.isLoggable(Level.FINE)) {
					logger.fine("Snapshot received from state machine for:" + snapshot.requestSeqNo + "(inst "
							+ snapshot.enclosingIntanceId + ")" + " (previous: " + previousSnapshotRequestSeqNo
							+ "(inst " + previousSnapshotInstanceId + ")) New size estimate: "
							+ _snapshotByteSizeEstimate.get());
				}
				
				_samplingRate = Math.max((snapshot.enclosingIntanceId - previousSnapshotInstanceId) / 5,
						Config.MIN_SNAPSHOT_SAMPLING);
			}
		});
	}

	/**
	 * Decides if a snapshot needs to be requested based on the current size of
	 * the log
	 */
	public void logSizeChanged(int newsize) {
		assert _dispatcher.amIInDispatcher() : "Only Dispatcher thread allowed. Called from "
				+ Thread.currentThread().getName();
		// logger.info("new log size: " + newsize);
		
		// TODO: Fix snapshotting. 
		// For the time being, disabled snapshotting for benchmarking
		if (ProcessDescriptor.getInstance().benchmarkRun) {
			// NS: Workaround to bug with snapshotting.
			if (newsize > 1000) {
				int nextID = _stableStorage.getLog().getNextId(); 
				_stableStorage.getLog().truncateBelow(Math.max(0, nextID-500));
			}
			return;
		}

		if (_askedForSnapshot && _forcedSnapshot) {
			return;
		}
		if ((_stableStorage.getLog().getNextId() - _lastSamplingInstance) < _samplingRate) {
			return;
		}
		_lastSamplingInstance = _stableStorage.getLog().getNextId();

		Snapshot lastSnapshot = _stableStorage.getLastSnapshot();
		int lastSnapshotInstance = lastSnapshot == null ? 0 : lastSnapshot.enclosingIntanceId;

		long logByteSize = _storage.getLog().byteSizeBetween(lastSnapshotInstance, _storage.getFirstUncommitted());

		if (logger.isLoggable(Level.FINE)) {
			logger.fine("Calculated log size for " + logByteSize);
		}

		// Don't do a snapshot if the log is too small
		if (logByteSize < Config.SNAPSHOT_MIN_LOG_SIZE) {
			return;
		}
	 
		
		if (!_askedForSnapshot) {
			if ((logByteSize / _snapshotByteSizeEstimate.get()) < Config.SNAPSHOT_ASK_RATIO) {
				return;
			}

			logger.fine("Asking state machine for shapshot");

			_snapshotProvider.askForSnapshot(lastSnapshotInstance);
			_askedForSnapshot = true;
			return;
		}

		// NUNO: Don't ever force snapshots.
		// JK: why? The service may just ignore it if it wants so.
		// It's just a second info for the service
		if (!_forcedSnapshot) {
			if ((logByteSize / _snapshotByteSizeEstimate.get()) < Config.SNAPSHOT_FORCE_RATIO) {
				return;
			}

			logger.fine("Forcing state machine to do shapshot");

			_snapshotProvider.forceSnapshot(lastSnapshotInstance);
			_forcedSnapshot = true;
			return;
		}

	}

	private final static Logger logger = Logger.getLogger(SnapshotMaintainer.class.getCanonicalName());
}
