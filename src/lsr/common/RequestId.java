package lsr.common;

import java.io.Serializable;

/**
 * Represents the unique id of request. To ensure uniqueness, the id contains
 * the id of client and the request sequence number. Every client should have
 * assigned unique client id, so that unique requests id can be created. Clients
 * gives consecutive sequence numbers to every sent request. The sequence number
 * starts with 0.
 */
public class RequestId implements Serializable, Comparable<RequestId> {
    private static final long serialVersionUID = 1L;

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
    public Long getClientId() {
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
        if (clientId != requestId.clientId)
            throw new IllegalArgumentException("Cannot compare requests from diffrents clients.");
        return seqNumber - requestId.seqNumber;
    }

    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;

        RequestId requestId = (RequestId) obj;
        return clientId == requestId.clientId && seqNumber == requestId.seqNumber;
    }

    public int hashCode() {
        return (int) (clientId ^ (clientId >>> 32)) ^ seqNumber;
    }

    public String toString() {
        return clientId + ":" + seqNumber;
    }
}
