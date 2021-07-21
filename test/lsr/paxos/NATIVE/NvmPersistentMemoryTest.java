package lsr.paxos.NATIVE;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.AccessDeniedException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import lsr.paxos.NvmCommonSetup;

public class NvmPersistentMemoryTest {
    
    public final String pmemDir = "/mnt/pmem";
    
    @Before
    public void checks() {
        NvmCommonSetup.initProcessDescr(3, 0);
        File dir = new File(pmemDir);
        assertTrue(pmemDir + " is not a directory", dir.isDirectory());
        assertTrue("Cannot write to " + pmemDir ,dir.canWrite());
        File file = new File(pmemDir, "JUNIT_PMEM_FILE");
        assertFalse("File " + file.getPath() + " exists", file.exists());
    }
    
    @Test
    public void loadTest() throws AccessDeniedException, UnsatisfiedLinkError {
        File file = new File(pmemDir, "JUNIT_PMEM_FILE");
        assertFalse(PersistentMemory.isLoaded());
        PersistentMemory.loadLib(file.getPath());
        assertTrue(PersistentMemory.isLoaded());
    }
    
    @After
    public void cleanup() {
        File file = new File(pmemDir, "JUNIT_PMEM_FILE");
        assertTrue("Failed to remove " + file.getPath(), file.delete());
    }

}
