package lsr.common;

import java.io.Serializable;

/*
 * For the same clientID, we can use the seqNumber to order the requests
 */
public class RequestId implements Serializable, Comparable<RequestId> {
    private static final long serialVersionUID = 1L;

    // private final Long _clientId;
    private final long clientId;
    private final int seqNumber;

    public RequestId(long clientId, int seqNumber) {
        // if (clientId == null)
        // throw new IllegalArgumentException("ClientId field cannot be null");
        this.clientId = clientId;
        this.seqNumber = seqNumber;
    }

    public Long getClientId() {
        return clientId;
    }

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
