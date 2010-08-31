package put.consensus;

import java.io.Serializable;
import java.util.SortedMap;

public interface Storage {

	/**
	 * Adds or modifies log entry identified by the given key.
	 * 
	 * The log should be implemented as persistent (surviving soft crashes)
	 * 
	 * @param key
	 *            - the ID of the log's entry
	 * @param value
	 *            - the value stored
	 * @throws StorageException
	 *             in case of any storage-related failures.
	 */
	void log(Serializable key, Serializable value) throws StorageException;

	/**
	 * Retrieves a previously recorded log entry. If no value has been stored
	 * under the given key, a null reference shall be returned.
	 * 
	 * @param key
	 *            - the ID of the log's entry
	 * @return the stored object or null reference
	 * @throws StorageException
	 *             in case of any storage-related failures.
	 */
	Object retrieve(Serializable key) throws StorageException;

	public int getHighestExecuteSeqNo();

	/** Retrieves a specific request */
	public Object getRequest(int requestNo);

	/**
	 * Returns all available executed requests <b>This may be dangerous, as the
	 * number of requests may be huge</b>
	 */
	public SortedMap<Integer, Object> getRequests();

	/** Returns specific range of requests */
	public SortedMap<Integer, Object> getRequests(int startingNo, int finishingNo);

}
