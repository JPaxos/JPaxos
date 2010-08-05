package lsr.paxos;

public class ReplicationException extends Exception {
	private static final long serialVersionUID = 1L;

	public ReplicationException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public ReplicationException(String msg) {
		super(msg);
	}
}
