package lsr.paxos.replica.storage;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import lsr.paxos.NvmTestTemplate;
import lsr.paxos.storage.PersistentConsensusInstance;

public class NvmExecuteUBTest extends NvmTestTemplate {
    public PersistentReplicaStorage storage = new PersistentReplicaStorage();

    @Test
    public void initial() {
        assertEquals(0, storage.decidedWaitingExecutionCount());
        assertEquals(null, storage.getDecidedWaitingExecution(2));
    }

    @Test
    public void simple() {
        assertEquals(0, storage.decidedWaitingExecutionCount());
        PersistentConsensusInstance ci = new PersistentConsensusInstance(4);
        storage.addDecidedWaitingExecution(ci.getId(), ci);
        assertEquals(ci, storage.getDecidedWaitingExecution(ci.getId()));
        assertEquals(1, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecution(ci.getId());
        assertEquals(0, storage.decidedWaitingExecutionCount());
    }

    @Test
    public void gap() {
        assertEquals(0, storage.decidedWaitingExecutionCount());
        PersistentConsensusInstance ci6 = new PersistentConsensusInstance(6);
        PersistentConsensusInstance ci8 = new PersistentConsensusInstance(8);
        storage.addDecidedWaitingExecution(ci6.getId(), ci6);
        storage.addDecidedWaitingExecution(ci8.getId(), ci8);
        assertEquals(2, storage.decidedWaitingExecutionCount());
        assertEquals(storage.getDecidedWaitingExecution(6), ci6);
        assertEquals(storage.getDecidedWaitingExecution(7), null);
        assertEquals(storage.getDecidedWaitingExecution(8), ci8);
        assertEquals(2, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecution(6);
        assertEquals(1, storage.decidedWaitingExecutionCount());
        storage.releaseDecidedWaitingExecution(8);
        assertEquals(0, storage.decidedWaitingExecutionCount());
    }
}
