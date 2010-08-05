package lsr.service;

import java.util.ArrayList;
import java.util.List;

import lsr.paxos.SnapshotListener;

/**
 * Abstract class which can be used to simplify creating new services. It adds
 * implementation for handling snapshot listeners.
 */
public abstract class AbstractService implements Service {
	/** Listeners which will be notified about new snapshot made by service */
	protected List<SnapshotListener> _listeners = new ArrayList<SnapshotListener>();

	public final void addSnapshotListener(SnapshotListener listener) {
		_listeners.add(listener);
	}

	public final void removeSnapshotListener(SnapshotListener listener) {
		_listeners.remove(listener);
	}

	/**
	 * Notifies all active listeners that new snapshot has been made.
	 * 
	 * @param instanceId
	 *            - the id of created snapshot
	 * @param snapshot
	 *            - the data containing snapshot
	 */
	protected void fireSnapshotMade(int instance, byte[] object) {
		for (SnapshotListener listener : _listeners)
			listener.onSnapshotMade(instance, object);
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
	@Override
	public void recoveryFinished() {
	}

}