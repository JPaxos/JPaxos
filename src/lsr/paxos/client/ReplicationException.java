package lsr.paxos.client;

/**
 * If client code did something not allowed (what may happen if the client
 * tampered with code) this exception can be thrown.
 * 
 * e.g., sending request no.3 after request no.6 will result in this exception
 */
public class ReplicationException extends Exception {
    private static final long serialVersionUID = 1L;

    public ReplicationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ReplicationException(String msg) {
        super(msg);
    }
}
