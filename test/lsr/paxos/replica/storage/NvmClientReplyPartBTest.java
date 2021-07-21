package lsr.paxos.replica.storage;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import lsr.paxos.NvmCommonSetup;

public class NvmClientReplyPartBTest {
    public PersistentReplicaStorage storage = new PersistentReplicaStorage();

    @BeforeClass
    public static void setup() throws Throwable {
        NvmCommonSetup.initProcessDescr(5, 1);
        NvmCommonSetup.loadPmem();
    }

    @Test
    public void partB() {
        NvmClientReplyPartATest.check(storage);
    }

    @AfterClass
    public static void cleanup() {
        NvmCommonSetup.removePmemFile();
    }
}
