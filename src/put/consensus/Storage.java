package put.consensus;

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
	void log(Object key, Object value) throws StorageException;

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
	Object retrieve(Object key) throws StorageException;

	/**
	 * Retrieves data about a consensus instance
	 * 
	 * @param instanceId
	 *            - the (global) sequence number
	 * @return a {@link ConsensusStateAndValue} for given instance or null
	 *         pointer if the instance does not exist
	 * @throws StorageException
	 *             in case of any storage-related failures.
	 */
	ConsensusStateAndValue instanceValue(Integer instanceId)
			throws StorageException;

	/**
	 * Returns the highest available instance
	 * 
	 * @return If there were any instances, this number indicated the highest
	 *         instanceID. Otherwise this function returns -1
	 */
	int highestInstance();
}
