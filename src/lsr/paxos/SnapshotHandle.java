package lsr.paxos;

public class SnapshotHandle {

	private final int currentPaxosInstanceId;
	
    public SnapshotHandle(int paxosInstanceId) { 
		this.currentPaxosInstanceId = paxosInstanceId;
		System.out.println("HANDLE created: "+paxosInstanceId);
	}

	public int getPaxosInstanceId(){
		return currentPaxosInstanceId;
    }
	
}
