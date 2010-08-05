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

}