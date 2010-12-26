package lsr.paxos.storage;

import java.io.IOException;

import lsr.common.DirectoryHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DiscWriterPerformanceTest {
    private String directoryPath = "bin/logs";

    @Before
    public void setUp() {
        DirectoryHelper.create(directoryPath);
    }

    @After
    public void tearDown() {
        DirectoryHelper.delete(directoryPath);
    }

    @Test
    public void fullSS() throws IOException {
        FullSSDiscWriter discWriter = new FullSSDiscWriter(directoryPath);
        long time = start(discWriter);
        discWriter.close();
        System.out.println("FullSS: " + time);
    }

    private long start(DiscWriter writer) {
        long startTime = System.currentTimeMillis();
        byte[] value = new byte[32024];
        for (int i = 0; i < 1000; i++) {
            writer.changeInstanceValue(i, i, value);
        }
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
}
