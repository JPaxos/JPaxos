package lsr.paxos;

import java.util.Iterator;
import java.util.Map;

import lsr.common.Reply;
import lsr.common.RequestId;

public class Snapshot {

	private byte[] data;
	private final int currentPaxosInstanceId;
	private final Map<Long,Reply> lastReplyForClient;
	
    public Snapshot(int paxosInstanceId, Map<Long,Reply> lastReplyForClient) { 
		currentPaxosInstanceId = paxosInstanceId;
		this.lastReplyForClient = lastReplyForClient;
	}

	public void setData(byte[] data){
		this.data = data;
    }
	
	public int getPaxosInstanceId(){
		return currentPaxosInstanceId;
    }
	
	public Map<Long,Reply> getReplyForClient(){
		return lastReplyForClient;
    }
	
}
