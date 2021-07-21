package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import lsr.paxos.NvmCommonSetup;

public class NvmStorageView_partA_Test {
    @BeforeClass
    public static void setup() throws Throwable {
        NvmCommonSetup.initProcessDescr(5, 1);
        NvmCommonSetup.loadPmem();
    }

    PersistentStorage storage = new PersistentStorage();

    @Test
    public void partA() {
        assertEquals(0, storage.getView());
        storage.setView(2);
        assertEquals(2, storage.getView());
        storage.setView(42);
        assertEquals(42, storage.getView());
    }
}
