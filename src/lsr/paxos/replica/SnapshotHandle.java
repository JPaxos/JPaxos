package lsr.paxos.replica;

public class SnapshotHandle { // implements Serializable

	private final int currentPaxosInstanceId;
	
    public SnapshotHandle(int paxosInstanceId) { 
		this.currentPaxosInstanceId = paxosInstanceId;
	}

	public int getPaxosInstanceId(){
		return currentPaxosInstanceId;
    }
	
}
