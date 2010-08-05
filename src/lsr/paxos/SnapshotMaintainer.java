package lsr.paxos;

import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Config;
import lsr.common.Dispatcher;
import lsr.common.MovingAverage;
import lsr.common.Pair;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.Storage;

/**
 * This class is informed when the log size is changed, asking the 
 * state machine (if necessary) for a snapshot.
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
	//	private int _samplingRate = 50;
	private int _samplingRate = 5;

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
	public void onSnapshotMade(final int instance, final byte[] snapshot) {
		// Called by the Replica thread. Queue it for execution on the Paxos dispatcher.
		_dispatcher.dispatch(new Runnable() {
			public void run() {
				if (logger.isLoggable(Level.FINE)) {
					logger.fine("Snapshot made. Instance: " + instance + ", log: " + _stableStorage.getLog().size());
				}
				
				int previousSnapshotInstance = _stableStorage.getLastSnapshotInstance();
				if (previousSnapshotInstance >= instance)
					return;

				_stableStorage.setLastSnapshot(
						new Pair<Integer, byte[]>(instance, snapshot));
				
				_stableStorage.getLog().truncateBelow(previousSnapshotInstance);
				_askedForSnapshot = _forcedSnapshot = false;
				_snapshotByteSizeEstimate.add(snapshot.length);

				if (logger.isLoggable(Level.FINE)) {
					logger.fine("Snapshot received from state machine for:" + instance + " (previous: " + previousSnapshotInstance
							+ ") New size estimate: " + _snapshotByteSizeEstimate.get());
				}

				_samplingRate = (instance - previousSnapshotInstance) / 5;
			}
		});
	}

	/**
	 * Decides if a snapshot needs to be requested based on 
	 * the current size of the log
	 */
	public void logSizeChanged(int newsize) {
		assert _dispatcher.amIInDispatcher() : 
			"Only Dispatcher thread allowed. Called from " + Thread.currentThread().getName();
		//		logger.info("new log size: " + newsize);

		if (_askedForSnapshot && _forcedSnapshot)
			return;

		if ((_stableStorage.getLog().getNextId() - _lastSamplingInstance) < _samplingRate)
			return;
		_lastSamplingInstance = _stableStorage.getLog().getNextId();

		int lastSnapshotInstance = _stableStorage.getLastSnapshotInstance();
		long logByteSize = _storage.getLog().byteSizeBetween(
				lastSnapshotInstance, 
				_storage.getFirstUncommitted());

		if (logger.isLoggable(Level.FINE)) {
			logger.fine("Calculated log size for " + logByteSize);
		}

		// Don't do a snapshot if the log is too small
		if (logByteSize < Config.SNAPSHOT_MIN_LOG_SIZE) {
			return;
		}

		if (!_askedForSnapshot) {
			if ((logByteSize / _snapshotByteSizeEstimate.get()) < Config.SNAPSHOT_ASK_RATIO){			
				return;
			}

			logger.fine("Asking state machine for shapshot");

			_snapshotProvider.askForSnapshot(lastSnapshotInstance);
			_askedForSnapshot = true;
			return;
		}

		// NUNO: Don't ever force snapshots.
//		if (!_forcedSnapshot) {
//			if ((logByteSize / _snapshotByteSizeEstimate.get()) < Config.SNAPSHOT_FORCE_RATIO) {				
//				return;
//			}
//
//			logger.fine("Forcing state machine to do shapshot");
//
//			_snapshotProvider.forceSnapshot(lastSnapshotInstance);
//			_forcedSnapshot = true;
//			return;
//		}

	}

	private final static Logger logger = Logger.getLogger(SnapshotMaintainer.class.getCanonicalName());
}
