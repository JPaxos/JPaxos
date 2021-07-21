package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import lsr.paxos.NvmCommonSetup;

public class NvmStorageView_partB_Test {

    PersistentStorage storage = new PersistentStorage();

    @BeforeClass
    public static void setup() throws Throwable {
        NvmCommonSetup.initProcessDescr(5, 1);
        NvmCommonSetup.loadPmem();
    }

    @Test
    public void partB() {
        assertEquals(42, storage.getView());
    }

    @AfterClass
    public static void cleanup() {
        NvmCommonSetup.removePmemFile();
    }
}
