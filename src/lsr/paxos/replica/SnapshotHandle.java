package lsr.paxos.replica;

import java.io.Serializable;

public class SnapshotHandle implements Serializable {

	private final int currentPaxosInstanceId;
	
    public SnapshotHandle(int paxosInstanceId) { 
		this.currentPaxosInstanceId = paxosInstanceId;
	}

	public int getPaxosInstanceId(){
		return currentPaxosInstanceId;
    }
	
}
