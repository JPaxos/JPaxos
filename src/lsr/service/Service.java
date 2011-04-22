package lsr.service;

import lsr.paxos.replica.Replica;
import lsr.paxos.replica.SnapshotListener;

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

    /**
     * Notifies service that it would be good to create snapshot now.
     * <code>Service</code> should check whether this is good moment, and create
     * snapshot if needed.
     * 
     * @param lastSnapshotNextRequestSeqNo - specified last known snapshot; is
     *            used to determine duplicate calling of method
     */
    void askForSnapshot(int lastSnapshotNextRequestSeqNo);

    /**
     * Notifies service that size of logs are much bigger than estimated size of
     * snapshot. Not implementing this method may cause slowing down the
     * algorithm, especially in case of network problems and also recovery in
     * case of crash can take more time.
     * 
     * @param lastSnapshotNextRequestSeqNo - specified last known snapshot; is
     *            used to determine duplicate calling of method
     */
    void forceSnapshot(int lastSnapshotNextRequestSeqNo);

    /**
     * Registers new listener which will be called every time new snapshot is
     * created by this <code>Service</code>.
     * 
     * @param listener - the listener to register
     */
    void addSnapshotListener(SnapshotListener listener);

    /**
     * Unregisters the listener from this service. It will not be called when
     * new snapshot is created by this <code>Service</code>.
     * 
     * @param listener - the listener to unregister
     */
    void removeSnapshotListener(SnapshotListener listener);

    /**
     * Restores the service state from snapshot
     * 
     * @param nextRequestSeqNo (last executed request sequential number + 1)
     *            before snapshot was made (i.e. next request to be executed no)
     * @param snapshot the snapshot itself
     */
    void updateToSnapshot(int nextRequestSeqNo, byte[] snapshot);
}