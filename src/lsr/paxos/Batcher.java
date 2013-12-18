package lsr.paxos;

import lsr.common.ClientRequest;
import lsr.common.RequestType;
import lsr.paxos.replica.DecideCallback;

public interface Batcher {

    // on/off

    public void start();

    public void suspendBatcher();

    public void resumeBatcher(int nextInstanceId);

    // input/output

    public void enqueueClientRequest(final RequestType request);

    /** Returns a batch or null if no batch is ready yet */
    public byte[] requestBatch();

    // informational

    public void instanceExecuted(int instanceId, ClientRequest[] requests);

    public void setDecideCallback(DecideCallback decideCallback);
}