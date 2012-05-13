package lsr.service;

import java.io.IOException;

import lsr.paxos.replica.Snapshot;

/**
 * This interface represents state machine with possibility to save current
 * state (snapshot). It is used by {@link Replica} to execute commands from
 * clients, for making snapshots, and also to update the state from other
 * snapshot (received from other replica).
 * <p>
 * All methods are called from the same thread, so it is not necessary to
 * synchronize them.
 * <p>
 * After creating new snapshot by this <code>Service</code>, all
 * <code>SnapshotListener</code> should be called. They shouldn't be called
 * after updating state machine from snapshot (see <code>updateToSnapshot</code>
 * for more details).
 * <p>
 * This interface provides two methods used for notifying that the snapshot
 * should be made. First is only asking for snapshot, which can be ignored if
 * for service it is bad moment for creating snapshot. Second methods is called
 * when logs are much bigger than estimated size of next snapshot.
 * <p>
 * For some methods, also request sequential number is provided. It is used to
 * determine when the snapshot by <code>Service</code> was made. The number of
 * created snapshot is last executed request.
 * 
 * @see Replica
 * @see SnapshotListener
 */
public interface Service {
	Snapshot snapshot = null;
	

    /**
     * Informs the service that the recovery process has been finished, i.e.
     * that the service is at least at the state later than by crashing.
     * 
     * Please notice, for some crash-recovery approaches this can mean that the
     * service is a lot further than by crash.
     */
	void recoveryFinished();

    /**
     * Executes one command from client on this state machine. This method will
     * be called by {@link Replica} in proper order. The number of request is
     * needed only for snapshot mechanism.
     * 
     * @param value - value of instance to execute on this service
     * @param executeSeqNo - ordinal number of this requests
     * @return generated reply which will be sent to client
     */
    byte[] execute(byte[] value, int executeSeqNo);
	
	public void installSnapshot(int paxosId, byte[] data);
	
	public byte[] takeSnapshot() throws IOException;
	
	public abstract Object makeObjectSnapshot();

}