package lsr.paxos;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class NvmTestTemplate {
    @BeforeClass
    public static void setup() throws Throwable {
        NvmCommonSetup.initProcessDescr(5, 1);
        NvmCommonSetup.loadPmem();
    }
    @AfterClass
    public static void cleanup() {
        NvmCommonSetup.removePmemFile();
    }
}
