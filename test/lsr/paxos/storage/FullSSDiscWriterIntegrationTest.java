package lsr.paxos.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FullSSDiscWriterIntegrationTest {
    private String directoryPath = "bin/logs";
    private File directory;

    @Before
    public void setUp() {
        // TODO TZ - use DirectoryHelper
        directory = new File(directoryPath);
        deleteDir(directory);
        directory.mkdirs();
    }

    @After
    public void tearDown() {
        if (!deleteDir(directory)) {
            throw new RuntimeException("Directory was not removed");
        }
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }

    @Test
    public void testInstanceViewChange() throws IOException {
        DiscWriter writer = new FullSSDiscWriter(directoryPath);
        writer.changeInstanceView(1, 2);

        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put((byte) 1); // type
        buffer.putInt(1); // id
        buffer.putInt(2); // view

        assertArrayEquals(buffer.array(), readFile(directory.getAbsolutePath() + "/sync.0.log"));

        writer.close();
    }

    @Test
    public void testInstanceValueChange() throws IOException {
        DiscWriter writer = new FullSSDiscWriter(directoryPath);

        byte[] value = new byte[] {1, 2};
        writer.changeInstanceValue(1, 2, value);

        ByteBuffer buffer = ByteBuffer.allocate(15);
        buffer.put((byte) 2); // type
        buffer.putInt(1); // id
        buffer.putInt(2); // view
        buffer.putInt(2); // value size
        buffer.put(value); // value

        String path = directory.getAbsolutePath() + "/sync.0.log";
        System.out.println(path);
        assertArrayEquals(readFile(path), buffer.array());

        writer.close();
    }

    @Test
    public void testGetNextLogNumber() throws IOException {
        String[] s = new String[] {"sync.0.log", "invalid", "sync.2.log", "sync.1.log"};

        FullSSDiscWriter writer = new FullSSDiscWriter(directoryPath);
        int lastLogNumber = writer.getLastLogNumber(s);
        assertEquals(lastLogNumber, 2);

        writer.close();
    }

    @Test
    public void changeViewNumber() throws IOException {
        DiscWriter writer = new FullSSDiscWriter(directoryPath);
        writer.changeViewNumber(5);
        writer.close();

        DataInputStream stream = new DataInputStream(new FileInputStream(directoryPath +
                                                                         "/sync.0.view"));
        int view = stream.readInt();
        stream.close();
        assertEquals(view, 5);
    }

    private byte[] readFile(String path) throws IOException {
        FileInputStream stream = new FileInputStream(path);
        int length = stream.available(); // danger
        byte[] value = new byte[length];
        stream.read(value);
        stream.close();
        return value;
    }

    @Test
    public void testLoad() throws IOException {
        DiscWriter writer = new FullSSDiscWriter(directoryPath);

        byte[] value = new byte[] {1, 2};
        byte[] newValue = new byte[] {1, 2, 3};
        writer.changeInstanceValue(1, 2, value);
        writer.changeInstanceValue(2, 2, value);
        writer.changeInstanceValue(1, 3, newValue);
        writer.changeInstanceView(1, 4);
        writer.close();

        writer = new FullSSDiscWriter(directoryPath);
        ConsensusInstance[] instances = writer.load().toArray(new ConsensusInstance[0]);
        writer.close();

        ConsensusInstance instance1 = instances[0];
        ConsensusInstance instance2 = instances[1];

        assertEquals(4, instance1.getView());
        assertArrayEquals(newValue, instance1.getValue());

        assertEquals(2, instance2.getView());
        assertArrayEquals(value, instance2.getValue());
    }

    @Test
    public void testLoadCorruptedData() throws IOException {
        DiscWriter writer = new FullSSDiscWriter(directoryPath);

        byte[] value = new byte[] {1, 2};
        byte[] newValue = new byte[] {1, 2, 3};
        writer.changeInstanceValue(1, 2, value);
        writer.changeInstanceValue(2, 2, value);
        writer.changeInstanceValue(1, 3, newValue);
        writer.changeInstanceView(1, 4);
        writer.close();

        FileOutputStream stream = new FileOutputStream(directoryPath + "/sync.0.log", true);
        stream.write(new byte[] {1, 2, 3});
        stream.close();

        writer = new FullSSDiscWriter(directoryPath);
        ConsensusInstance[] instances = writer.load().toArray(new ConsensusInstance[0]);
        writer.close();

        ConsensusInstance instance1 = instances[0];
        ConsensusInstance instance2 = instances[1];

        assertEquals(4, instance1.getView());
        assertArrayEquals(newValue, instance1.getValue());

        assertEquals(2, instance2.getView());
        assertArrayEquals(value, instance2.getValue());
    }

    @Test
    public void loadDataFromMoreFiles() throws IOException {
        DiscWriter writer = new FullSSDiscWriter(directoryPath);

        byte[] value = new byte[] {1, 2};
        byte[] newValue = new byte[] {1, 2, 3};
        writer.changeInstanceValue(1, 2, value);
        writer.changeInstanceValue(2, 2, value);
        writer.changeInstanceValue(1, 3, newValue);
        writer.close();

        writer = new FullSSDiscWriter(directoryPath);
        writer.changeInstanceView(1, 4);
        writer.close();

        writer = new FullSSDiscWriter(directoryPath);
        ConsensusInstance[] instances = writer.load().toArray(new ConsensusInstance[0]);
        writer.close();

        ConsensusInstance instance1 = instances[0];
        ConsensusInstance instance2 = instances[1];

        assertEquals(4, instance1.getView());
        assertArrayEquals(newValue, instance1.getValue());

        assertEquals(2, instance2.getView());
        assertArrayEquals(value, instance2.getValue());
    }

    @Test
    public void loadViewNumber() throws IOException {
        DiscWriter writer = new FullSSDiscWriter(directoryPath);
        writer.changeViewNumber(5);
        writer.close();

        writer = new FullSSDiscWriter(directoryPath);
        int view = writer.loadViewNumber();
        writer.close();

        assertEquals(view, 5);
    }
}
