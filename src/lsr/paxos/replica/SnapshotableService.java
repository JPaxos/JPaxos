package lsr.paxos.replica;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lsr.common.Reply;
import lsr.common.Request;
import lsr.paxos.Snapshot;

/* 
 * 
 * The service can perform the snapshot in the following ways:
 * 
 * - passive service: when executeRequest() is called, the service
 * does a snapshot using the Replica thread. The execute lock has no
 * effect on this case, thread-safety is ensured and the snapshot
 * is trivially synchronized with the execution of requests, meaning
 * no more requests are executed until the snapshot is done.
 * 
 * - active service: it can perform a snapshot at any time using
 * its own thread. To do so, it calls the startSnapshot() method,
 * which will acquire a lock, preventing any further requests from
 * being executed. This method also provides a SnapshotContext to the
 * service, which the service must fill with the state corresponding
 * to the context. This is easy, no additional requests are executed
 * during this period. When the service finishes the snapshot, it
 * calls finishSnapshot(), which passes the snapshot to the 
 * Replica and releases the execution lock.
 * 
 * This is a simple approach, but it is possible to extend it to 
 * allow the service to continue executing requests while doing
 * the snapshot. This will put more responsibility on the side
 * of the service, which must now synchronize execution with 
 * snapshotting. 
 *  
 * @author Nuno Santos (LSR)
 *
 */
abstract public class  SnapshotableService {
	private final Replica replica; 

	public SnapshotableService(Replica r) {
		this.replica = r;
	}

	/* Protects access to the fields _executedRequests, _executeSeqNo, _lastInstance.
	 */
	private final ReentrantLock lock = new ReentrantLock();
	private final Map<Long, Integer> _executedRequests = new HashMap<Long, Integer>();
	private int _executeSeqNo = 0;
	private int _lastInstance = 0;

	/* Called by Replica. Package access hides this method from the service implementation
	 */
	final Reply handleRequest(Request request, int instanceID) {
		lock.lock();
		try {
			_lastInstance = instanceID;
			Integer lastSequenceNumberFromClient = 
				_executedRequests.get(request.getRequestId().getClientId());
			if (lastSequenceNumberFromClient != null
					&& request.getRequestId().getSeqNumber() <= lastSequenceNumberFromClient) {
				// Do not execute the same request several times.
				return null;
			}

			byte[] result = execute(request.getValue(), instanceID, _executeSeqNo);
			_executeSeqNo++;

			return new Reply(request.getRequestId(), result);
		} finally {
			lock.unlock();
		}
	}

	/*
	 * Called by the Service when it wants to do a snapshot.
	 * Passes a Snapshot instance to the service implementation containing
	 * the context of the snapshot, that is, the map of executed requests,
	 * the last sequence number and the last instance id. The service must 
	 * supply the field value with the corresponding serialized state.
	 * 
	 * The lock ensures that no requests are executed between the time
	 * the service starts a snapshot until the time it finishes the snapshot.
	 * This is a very crude way of ensuring that the state of the service
	 * does not change while it is making a snapshot. It's an initial proof
	 * of concept that can be improved as we understand better the application
	 * requirements. 
	 */
	public final Snapshot startSnapshot() {
		lock.lock();
		Snapshot snapshot = new Snapshot();
		snapshot.lastRequestIdForClient = new HashMap<Long, Integer>(_executedRequests);
		snapshot.requestSeqNo = _executeSeqNo;
		snapshot.instanceId = _lastInstance;   
		return snapshot;
	}

	/* 
	 * Called by the service when it finishes building a snapshot.
	 * This releases the lock and allows the service to resume executing
	 * requests.
	 */
	public final void finishSnapshot(Snapshot s) {
		lock.unlock();
		replica.onSnapshotMade(s);
	}
	

	/* methods to be implemented by service. */
	abstract byte[] execute(byte[] value, int instanceId, int executeSeqNo);
	abstract void instanceExecuted(int instanceId);
	abstract void updateToSnapshot(int requestSeqNo, byte[] snapshot);
	abstract void recoveryFinished();

	abstract void askForSnapshot(int lastSnapshotRequestSeqNo);
	abstract void forceSnapshot(int lastSnapshotRequestSeqNo);
}
