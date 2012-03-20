package lsr.common;

import java.io.Serializable;

/**
 * Represents the unique id of request. To ensure uniqueness, the id contains
 * the id of client and the request sequence number. Every client should have
 * assigned unique client id, so that unique requests id can be created. Clients
 * gives consecutive sequence numbers to every sent request. The sequence number
 * starts with 0.
 */
public final class RequestId implements Serializable, Comparable<RequestId> {
    private static final long serialVersionUID = 1L;

    /** Represents the no-op request. */
    public static final RequestId NOP = new RequestId(-1, -1);

    private final long clientId;
    private final int seqNumber;

    /**
     * Creates new <code>RequestId</code> instance.
     * 
     * @param clientId - the id of client
     * @param seqNumber - the request sequence number
     */
    public RequestId(long clientId, int seqNumber) {
        this.clientId = clientId;
        this.seqNumber = seqNumber;
    }

    /**
     * Returns the id of client.
     * 
     * @return the id of client
     */
    public long getClientId() {
        return clientId;
    }

    /**
     * Returns the request sequence number.
     * 
     * @return the request sequence number
     */
    public int getSeqNumber() {
        return seqNumber;
    }

    public int compareTo(RequestId requestId) {
        if (clientId != requestId.clientId) {
            throw new IllegalArgumentException("Cannot compare requests from diffrents clients.");
        }
        return seqNumber - requestId.seqNumber;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof RequestId)) 
            return false;

        RequestId requestId = (RequestId) obj;
        return clientId == requestId.clientId && seqNumber == requestId.seqNumber;
    }

    public int hashCode() {
        int result = 17;
        result = 31*result+(int)clientId;
        result = 31*result+seqNumber;
        return result;
    }

    public boolean isNop() {
        return clientId == -1 && seqNumber == -1;
    }

    public String toString() {
        return isNop() ? "nop" : clientId + ":" + seqNumber;
    }
}
