package lsr.paxos;

import java.util.List;

import lsr.common.Dispatcher;
import lsr.common.Request;
import lsr.paxos.events.ProposeEvent;
import lsr.paxos.events.StartProposerEvent;
import lsr.paxos.storage.Storage;

public interface Paxos {
	public Dispatcher getDispatcher();

	/**
	 * Gets the id of the replica which is currently the leader.
	 * 
	 * @return id of replica which is leader
	 */
	public int getLeaderId();

	/**
	 * Is this process on the role of leader?
	 * 
	 * @return <code>true</code> if current process is the leader;
	 *         <code>false</code> otherwise
	 */
	public boolean isLeader();

	/**
	 * Adds {@link ProposeEvent} to current dispatcher which starts proposing
	 * new value by <code>Proposer</code> of this replica. This replica has to
	 * by a leader to call this method.
	 * 
	 * @param value
	 *            - the object to propose in new consensus
	 * 
	 * @throws NotLeaderException
	 */
	public void propose(Request request) throws NotLeaderException;

	void decide(int id);

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

	void advanceView(int newView);

	Storage getStorage();

	List<Request> extractValueList(byte[] value);

	public CatchUp getCatchup();

	public void onSnapshotMade(Snapshot snapshot);
}