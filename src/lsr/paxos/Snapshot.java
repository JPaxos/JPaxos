package lsr.paxos;

import java.util.Map;

import lsr.common.Reply;
import lsr.common.RequestId;

public class Snapshot {

	private byte[] data;
	private Map<Long,Reply> lastReplyForClient;
	private SnapshotHandle handle;
	//private final int currentPaxosInstanceId;
	
    public Snapshot(int paxosInstanceId, Map<Long,Reply> lastReplyForClient) { 
		SnapshotHandle handle = new SnapshotHandle(paxosInstanceId);
		this.lastReplyForClient = lastReplyForClient;
		//this.currentPaxosInstanceId = paxosInstanceId;
	}
	
	public Snapshot(SnapshotHandle handle, Map<Long,Reply> lastReplyForClient) { 
		this.handle = handle;
		this.lastReplyForClient = lastReplyForClient;
		//this.currentPaxosInstanceId = handle.getPaxosInstanceId();
	}
	
	/*public int getPaxosInstanceId(){
		return currentPaxosInstanceId;
    }*/

	public void setData(byte[] data){
		this.data = data;
    }
	
	public SnapshotHandle getHandle(){
		return handle;
    }
	
	public void setReplyForClient(Map<Long,Reply> lastReplyForClient){
		this.lastReplyForClient = lastReplyForClient;
    }
	
	public Map<Long,Reply> getReplyForClient(){
		return lastReplyForClient;
    }
	
}
