package lsr.paxos.replica;

import java.util.Map;

import lsr.common.Reply;
import lsr.common.RequestId;

public class Snapshot { // implements Serializable

	private byte[] data;
	private Map<Long,Reply> lastReplyForClient;
	private final SnapshotHandle handle;
	private final boolean localSnapshot;
	
    public Snapshot(int paxosInstanceId, Map<Long,Reply> lastReplyForClient, boolean localSnapshot) { 
		this.handle = new SnapshotHandle(paxosInstanceId);
		this.lastReplyForClient = lastReplyForClient;
		this.localSnapshot = localSnapshot;
	}
	
	public Snapshot(SnapshotHandle handle, Map<Long,Reply> lastReplyForClient, boolean localSnapshot) { 
		this.handle = handle;
		this.lastReplyForClient = lastReplyForClient;
		this.localSnapshot = localSnapshot;
	}

	public void setData(byte[] data){
		this.data = data;
    }
	
	public byte[] getData(){
		return data;
    }
	
	public boolean isLocal(){
		return localSnapshot;
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
