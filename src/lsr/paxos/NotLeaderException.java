package lsr.paxos;

public class NotLeaderException extends Exception {
    private static final long serialVersionUID = 1L;

    public NotLeaderException(String msg) {
        super(msg);
    }
}
