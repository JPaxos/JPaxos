package lsr.paxos;

import java.util.Map;

import lsr.common.Reply;
import lsr.common.RequestId;

public class Snapshot {

	private byte[] data;
	private final Map<Long,Reply> lastReplyForClient;
	private SnapshotHandle handle;
	
    public Snapshot(int paxosInstanceId, Map<Long,Reply> lastReplyForClient) { 
		System.out.println("SNAPSHOT created");
		SnapshotHandle handle = new SnapshotHandle(paxosInstanceId);
		this.lastReplyForClient = lastReplyForClient;
	}
	
	public Snapshot(SnapshotHandle handle, Map<Long,Reply> lastReplyForClient) { 
		System.out.println("SNAPSHOT created");
		this.handle = handle;
		this.lastReplyForClient = lastReplyForClient;
	}

	public void setData(byte[] data){
		this.data = data;
    }
	
	public SnapshotHandle getHandle(){
		return handle;
    }
	
	public Map<Long,Reply> getReplyForClient(){
		return lastReplyForClient;
    }
	
}
