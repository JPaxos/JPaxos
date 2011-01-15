package lsr.paxos;

import lsr.common.Dispatcher;
import lsr.common.Request;
import lsr.paxos.events.ProposeEvent;
import lsr.paxos.events.StartProposerEvent;
import lsr.paxos.network.Network;
import lsr.paxos.storage.Storage;

public interface Paxos {
    /**
     * Gets the dispatcher used by paxos to avoid concurrency in handling
     * events.
     * 
     * @return current dispatcher object
     */
    Dispatcher getDispatcher();

    /**
     * Gets the id of the replica which is currently the leader.
     * 
     * @return id of replica which is leader
     */
    int getLeaderId();

    /**
     * Is this process on the role of leader?
     * 
     * @return <code>true</code> if current process is the leader;
     *         <code>false</code> otherwise
     */
    boolean isLeader();

    /**
     * Adds {@link ProposeEvent} to current dispatcher which starts proposing
     * new value by <code>Proposer</code> of this replica. This replica has to
     * by a leader to call this method.
     * 
     * @param request - the object to propose in new consensus
     * 
     * @throws NotLeaderException if current process is not a leader
     */
    void propose(Request request) throws NotLeaderException;

    /**
     * Changes state of specified consensus instance to <code>DECIDED</code>.
     * 
     * @param instanceId - the id of instance that has been decided
     */
    void decide(int instanceId);

    /**
     * Adds {@link StartProposerEvent} to current dispatcher which starts the
     * proposer on current replica.
     */
    void startProposer();

    /**
     * Starts Paxos - the protocol should not start before some of the recovery
     * processes are still running
     */
    void startPaxos();

    /**
     * Increases the view of this process to specified value. The new view has
     * to be greater than the current one.
     * 
     * @param newView - the new view number
     */
    void advanceView(int newView);

    /**
     * Returns the storage with the current state of paxos protocol.
     * 
     * @return the storage
     */
    Storage getStorage();

    /**
     * Returns the catch-up mechanism used by paxos protocol.
     * 
     * @return the catch-up mechanism
     */
    CatchUp getCatchup();

    Proposer getProposer();

    void onSnapshotMade(Snapshot snapshot);

    Network getNetwork();
}