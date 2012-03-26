package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import lsr.common.DirectoryHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SingleNumberWriterTest {
    private static final String FILE_NAME = "file.txt";
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
    public void shouldReturnZeroIfFileNotExist() {
        SingleNumberWriter writer = new SingleNumberWriter(directoryPath, FILE_NAME);
        assertEquals(0, writer.readNumber());
    }

    @Test
    public void shouldWriteNewNumber() {
        SingleNumberWriter writer = new SingleNumberWriter(directoryPath, FILE_NAME);
        writer.writeNumber(5);
        assertEquals(5, writer.readNumber());
    }

    @Test
    public void shouldReadOldValueAfterCrash() {
        SingleNumberWriter writer = new SingleNumberWriter(directoryPath, FILE_NAME);
        writer.writeNumber(5);

        SingleNumberWriter newWriter = new SingleNumberWriter(directoryPath, FILE_NAME);
        assertEquals(5, newWriter.readNumber());
    }

    @Test
    public void shouldOverwriteWrittenValue() {
        SingleNumberWriter writer = new SingleNumberWriter(directoryPath, FILE_NAME);
        writer.writeNumber(5);
        writer.writeNumber(10);

        SingleNumberWriter newWriter = new SingleNumberWriter(directoryPath, FILE_NAME);
        assertEquals(10, newWriter.readNumber());
    }

    @Test
    public void shouldCreateConsecutiveFiles() {
        SingleNumberWriter writer = new SingleNumberWriter(directoryPath, FILE_NAME);
        writer.writeNumber(5);
        assertTrue(new File(directoryPath, FILE_NAME + ".0").exists());
        writer.writeNumber(10);
        assertFalse(new File(directoryPath, FILE_NAME + ".0").exists());
        assertTrue(new File(directoryPath, FILE_NAME + ".1").exists());

        SingleNumberWriter newWriter = new SingleNumberWriter(directoryPath, FILE_NAME);
        assertEquals(10, newWriter.readNumber());
    }
}
