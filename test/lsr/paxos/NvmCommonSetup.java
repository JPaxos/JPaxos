package lsr.paxos;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;

import lsr.common.Configuration;
import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.NATIVE.PersistentMemory;

public class NvmCommonSetup {
    /* this is a static class */
    private NvmCommonSetup() {
        throw new RuntimeException();
    }

    public static final String pmemDir = "/mnt/pmem";

    public static void initProcessDescr(int n, int localid) {
        assert (localid < n);
        ArrayList<PID> processes = new ArrayList<>();
        for (int i = 0; i < n; ++i)
            processes.add(new PID(i, String.format("127.0.%d.1", i), 2000 + i, 3000 + i));
        ProcessDescriptor.initialize(new Configuration(processes), localid);
    }

    public static void loadPmem() throws Throwable {
        File dir = new File(pmemDir);
        assertTrue(pmemDir + " is not a directory", dir.isDirectory());
        assertTrue("Cannot write to " + pmemDir, dir.canWrite());
        File file = new File(pmemDir, "JUNIT_PMEM_FILE");
        assertFalse(PersistentMemory.isLoaded());
        PersistentMemory.loadLib(file.getPath());
        assertTrue(PersistentMemory.isLoaded());
    }

    public static void removePmemFile() {
        File file = new File(pmemDir, "JUNIT_PMEM_FILE");
        assertTrue("Failed to remove " + file.getPath(), file.delete());
    }
}
