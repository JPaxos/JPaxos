package lsr.paxos.replica;

import java.io.Serializable;
import java.util.Map;

import lsr.common.Reply;
import lsr.common.RequestId;

public class Snapshot implements Serializable{

	private byte[] data;
	private Map<Long,Reply> lastReplyForClient;
	private final SnapshotHandle handle;
	
    public Snapshot(int paxosInstanceId, Map<Long,Reply> lastReplyForClient) { 
		this.handle = new SnapshotHandle(paxosInstanceId);
		this.lastReplyForClient = lastReplyForClient;
	}
	
	public Snapshot(SnapshotHandle handle, Map<Long,Reply> lastReplyForClient) { 
		this.handle = handle;
		this.lastReplyForClient = lastReplyForClient;
	}

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
