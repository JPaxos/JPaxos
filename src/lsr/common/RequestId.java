package lsr.common;

import java.io.Serializable;

/*
 * For the same clientID, we can use the seqNumber to order the requests 
 */
public class RequestId implements Serializable, Comparable<RequestId> {
	private static final long serialVersionUID = 1L;

//	private final Long _clientId;
	private final long _clientId;
	private final int _seqNumber;

	public RequestId(long clientId, int seqNumber) {
//		if (clientId == null)
//			throw new IllegalArgumentException("ClientId field cannot be null");
		_clientId = clientId;
		_seqNumber = seqNumber;
	}

	public Long getClientId() {
		return _clientId;
	}

	public int getSeqNumber() {
		return _seqNumber;
	}

	public int compareTo(RequestId requestId) {
		if (_clientId != requestId._clientId)
			throw new IllegalArgumentException("Cannot compare requests from diffrents clients.");
		return _seqNumber - requestId._seqNumber;
	}

	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;

		RequestId requestId = (RequestId) obj;
		return _clientId == requestId._clientId && _seqNumber == requestId._seqNumber;
	}

	public int hashCode() {
		return (int)(_clientId ^ (_clientId >>> 32)) ^ _seqNumber;
	}

	public String toString() {
		return _clientId + ":" + _seqNumber;
	}
}
