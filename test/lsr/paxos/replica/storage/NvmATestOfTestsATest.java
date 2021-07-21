package lsr.paxos.replica.storage;

import org.junit.Test;

import lsr.paxos.NvmCommonSetup;

public class NvmATestOfTestsATest {
    @Test
    public void partA() throws Throwable {
        NvmCommonSetup.initProcessDescr(5, 1);
        NvmCommonSetup.loadPmem();
    }
}
