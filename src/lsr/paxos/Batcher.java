package lsr.paxos;

import lsr.common.RequestType;

public interface Batcher {
    public boolean enqueueClientRequest(RequestType request);

    public byte[] requestBatch();

    public void suspendBatcher();

    public void resumeBatcher(int nextInstanceId);
    
    public void instanceExecuted(int instanceId, AugmentedBatch augmentedBatch);
}