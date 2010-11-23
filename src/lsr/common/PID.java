package lsr.common;

/**
 * Keeps data related with replica like id, hostname and port numbers required
 * for replicas to communicate with each other, and by clients to connect to
 * replicas.
 */
public class PID {
	private final String hostname;
	private final int replicaPort;
	private final int clientPort;
	private final int id;

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
		this.id = id;
		this.clientPort = clientPort;
		this.hostname = hostname;
		this.replicaPort = replicaPort;
	}

	/**
	 * Returns the hostname used by replicas and clients to connect.
	 * 
	 * @return the name of host
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * Return the port number used by other replicas to connect to this replica.
	 * 
	 * @return port number
	 */
	public int getReplicaPort() {
		return replicaPort;
	}

	/**
	 * Returns the port number used by client to connect to this replica.
	 * 
	 * @return port number
	 */
	public int getClientPort() {
		return clientPort;
	}

	/**
	 * Returns unique id of this replica.
	 * 
	 * @return process id
	 */
	public int getId() {
		return id;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || (obj.getClass() != this.getClass()))
			return false;

		PID pid = (PID) obj;
		return hostname.equals(pid.hostname)
				&& replicaPort == pid.replicaPort;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 31 * hash + hostname.hashCode();
		hash = 31 * hash + replicaPort;
		hash = 31 * hash + clientPort;
		hash = 31 * hash + id;
		return hash;
	}

	@Override
	public String toString() {
		return "[p" + id + "] " + hostname + ", ports = (replica="
				+ replicaPort + ", client=" + clientPort + ")";
	}
}
