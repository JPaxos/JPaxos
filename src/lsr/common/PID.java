package lsr.common;

/**
 * Keeps data related with replica like id, hostname and port numbers required
 * for replicas to communicate with each other, and by clients to connect to
 * replicas.
 */
public class PID {
	private final String _hostname;
	private final int _replicaPort;
	private final int _clientPort;
	private final int _id;

	/**
	 * Creates new process identifier.
	 * 
	 * @param id
	 *            - process id
	 * @param hostname
	 *            - name of host used to connect to this process
	 * @param replicaPort
	 *            - port number used by other replicas
	 * @param clientPort
	 *            - port number used by clients
	 */
	public PID(int id, String hostname, int replicaPort, int clientPort) {
		if (hostname == null)
			throw new NullPointerException("Hostname field cannot be null");
		_id = id;
		_clientPort = clientPort;
		_hostname = hostname;
		_replicaPort = replicaPort;
	}

	/**
	 * Returns the hostname used by replicas and clients to connect.
	 * 
	 * @return the name of host
	 */
	public String getHostname() {
		return _hostname;
	}

	/**
	 * Return the port number used by other replicas to connect to this replica.
	 * 
	 * @return port number
	 */
	public int getReplicaPort() {
		return _replicaPort;
	}

	/**
	 * Returns the port number used by client to connect to this replica.
	 * 
	 * @return port number
	 */
	public int getClientPort() {
		return _clientPort;
	}

	/**
	 * Returns unique id of this replica.
	 * 
	 * @return process id
	 */
	public int getId() {
		return _id;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || (obj.getClass() != this.getClass()))
			return false;

		PID pid = (PID) obj;
		return _hostname.equals(pid._hostname)
				&& _replicaPort == pid._replicaPort;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 31 * hash + _hostname.hashCode();
		hash = 31 * hash + _replicaPort;
		hash = 31 * hash + _clientPort;
		hash = 31 * hash + _id;
		return hash;
	}

	@Override
	public String toString() {
		return "[p" + _id + "] " + _hostname + ", ports = (replica="
				+ _replicaPort + ", client=" + _clientPort + ")";
	}
}
