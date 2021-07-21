package lsr.paxos.replica.storage;

import static org.junit.Assert.assertFalse;

import org.junit.Test;

import lsr.paxos.NvmCommonSetup;
import lsr.paxos.NATIVE.PersistentMemory;

public class NvmATestOfTestsBTest {
    @Test
    public void partB() throws Throwable {
        assertFalse(PersistentMemory.isLoaded());
        NvmCommonSetup.removePmemFile();
    }
}
