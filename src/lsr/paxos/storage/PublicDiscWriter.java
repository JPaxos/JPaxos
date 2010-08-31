package lsr.paxos.storage;

import java.io.Serializable;

public interface PublicDiscWriter {

	/**
	 * Synchronous method for recording a key;value pair. Calling this method
	 * again with the same key causes overwriting previous value. Old values do
	 * stay in logs and make the logs bigger.
	 * 
	 * This method uses java serialization to record objects on disk.
	 */

	public void record(Serializable key, Serializable value);

	/**
	 * Retrieves previously recorded key;value pair.
	 */
	public Object retrive(Serializable key);

}