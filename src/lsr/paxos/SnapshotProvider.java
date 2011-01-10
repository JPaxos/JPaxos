package lsr.paxos;

/**
 * This interface represents state machine with possibility to save current
 * state (snapshot). It is used by implementations of a replica to execute
 * commands from clients, for making snapshots, and also to update the state
 * from other snapshot (received from other replica).
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
 * For some methods, also instance number is provided. It is used to determine
 * when the snapshot by <code>Service</code> was made. The number of created
 * snapshot is first <b>not</b> executed instance. If last executed instance
 * before snapshot was <code>x</code> then snapshot number is <code>x+1</code>.
 */
public interface SnapshotProvider {

    /**
     * Notifies service that it would be good to create snapshot now.
     * <code>Service</code> should check whether this is good moment, and create
     * snapshot if needed.
     */
    void askForSnapshot();

    /**
     * Notifies service that size of logs are much bigger than estimated size of
     * snapshot. Not implementing this method may cause slowing down the
     * algorithm, especially in case of network problems and also recovery in
     * case of crash can take more time.
     */
    void forceSnapshot();

    /**
     * Restore the service state from the snapshot provided. This is used by the
     * catch-up mechanism.
     * 
     * @param snapshot Starts by the map of client id to the request id of the
     *            last request executed, followed by the state of the service
     */
    public void handleSnapshot(Snapshot snapshot);

}