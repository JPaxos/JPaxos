package lsr.paxos.storage;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import lsr.common.ClientRequest;
import lsr.common.CrashModel;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.ForwardClientBatch;
import lsr.paxos.replica.ClientBatchID;
import lsr.paxos.replica.ClientBatchManager;

/**
 * Singleton class. Why? As it would be utterly wrong to have two such objects.
 */
public class ClientBatchStore {

    public static ClientBatchStore instance = __init();

    private static ClientBatchStore __init() {
        if (CrashModel.FullSS.equals(ProcessDescriptor.processDescriptor.crashModel))
            return new SynchronousClientBatchStore();
        else
            return new ClientBatchStore();
    }

    protected ClientBatchStore() {
        assert ProcessDescriptor.processDescriptor.indirectConsensus;
        // nop is rare, better put it on the map than check at each search
        batches.put(ClientBatchID.NOP, new ClientRequest[0]);
    }

    protected final HashMap<ClientBatchID, ClientRequest[]> batches = new HashMap<ClientBatchID, ClientRequest[]>();
    protected final HashSet<ClientBatchID> batchesWaitedFor = new HashSet<ClientBatchID>();

    // TODO: garbage collect not instanced batches on followers
    protected final HashSet<ClientBatchID> instancelessBatches = new HashSet<ClientBatchID>();

    protected ClientBatchManager clientBatchManager = null;

    public synchronized ClientRequest[] getBatch(ClientBatchID batchId) {
        return batches.get(batchId);
    }

    /**
     * Maps batch ID -> instance and returns if the value is already available
     */
    public synchronized void associateWithInstance(ClientBatchID batchId) {
        if (batches.containsKey(batchId)) {
            instancelessBatches.remove(batchId);
            return;
        }

        batchesWaitedFor.add(batchId);
    }

    public synchronized void setBatch(final ClientBatchID batchId, ClientRequest[] value) {
        batches.put(batchId, value);
        if (!batchesWaitedFor.remove(batchId))
            instancelessBatches.add(batchId);
    }

    public synchronized void setBatch(ForwardClientBatch fReq) {
        setBatch(fReq.rid, fReq.requests);
    }

    public synchronized boolean isAnyInstanceWaiting(ClientBatchID batchId) {
        return batchesWaitedFor.contains(batchId);
    }

    public ClientBatchManager getClientBatchManager() {
        return clientBatchManager;
    }

    /**
     * Delivers batch manager for easy access to it
     */
    public void setClientBatchManager(ClientBatchManager clientBatchManager) {
        assert clientBatchManager != null;
        assert this.clientBatchManager == null;
        this.clientBatchManager = clientBatchManager;
    }

    /**
     * Get batch ID's of available batches that are not yet associated with any
     * instance
     * 
     * Returns cloned object.
     */
    @SuppressWarnings("unchecked")
    public synchronized HashSet<ClientBatchID> getInstancelessBatches() {
        return (HashSet<ClientBatchID>) instancelessBatches.clone();
    }

    public synchronized boolean hasAllBatches(Collection<ClientBatchID> cbids) {
        for (ClientBatchID cbid : cbids) {
            if (!batches.containsKey(cbid))
                return false;
        }
        return true;
    }

    public synchronized void removeBatches(Collection<ClientBatchID> cbids) {
        for (ClientBatchID cbid : cbids)
            batches.remove(cbid);

        // after updating to a snapshot it may happen
        batchesWaitedFor.removeAll(cbids);
        instancelessBatches.removeAll(cbids);

        if (clientBatchManager != null) {
            clientBatchManager.removeBatches(cbids);
        }
    }
}
