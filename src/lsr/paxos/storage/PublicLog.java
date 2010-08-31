package lsr.paxos.storage;

import java.util.SortedMap;

public interface PublicLog {

	/** Returns read-only access to the log */
	public SortedMap<Integer, ConsensusInstance> getInstanceMap();

	/** Informs how many requests were executed on the state machine */
	public int getHighestExecuteSeqNo();

	/** Retrieves a specific request */
	public byte[] getRequest(int requestNo);

	/** 
	 * Returns all available executed requests
	 * <b>This may be dangerous, as the number of requests may be huge</b> 
	 */
	public SortedMap<Integer, byte[]> getRequests();

	/** Returns specific range of requests */
	public SortedMap<Integer, byte[]> getRequests(int startingNo, int finishingNo);

}