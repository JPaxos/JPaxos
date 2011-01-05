package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;

import lsr.common.DirectoryHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SingleNumberWriterTest {
    private static final String FILE_NAME = "file.txt";
    private String directoryPath = "bin/logs";
    private String filePath;

    @Before
    public void setUp() {
        DirectoryHelper.create(directoryPath);

        filePath = new File(directoryPath, FILE_NAME).getAbsolutePath();
    }

    @After
    public void tearDown() {
        DirectoryHelper.delete(directoryPath);
    }

    @Test
    public void shouldReturnZeroIfFileNotExist() {
        SingleNumberWriter writer = new SingleNumberWriter(filePath);
        assertEquals(0, writer.readNumber());
    }

    @Test
    public void shouldWriteNewNumber() {
        SingleNumberWriter writer = new SingleNumberWriter(filePath);
        writer.writeNumber(5);
        assertEquals(5, writer.readNumber());
    }

    @Test
    public void shouldReadOldValueAfterCrash() {
        SingleNumberWriter writer = new SingleNumberWriter(filePath);
        writer.writeNumber(5);

        SingleNumberWriter newWriter = new SingleNumberWriter(filePath);
        assertEquals(5, newWriter.readNumber());
    }
}
