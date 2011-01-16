package lsr.paxos.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lsr.common.DirectoryHelper;
import lsr.common.Reply;
import lsr.common.RequestId;
import lsr.paxos.Snapshot;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FullSSDiscWriterTest {
    private String directoryPath = "bin/logs";
    private FullSSDiscWriter writer;

    @Before
    public void setUp() throws FileNotFoundException {
        DirectoryHelper.create(directoryPath);
        writer = new FullSSDiscWriter(directoryPath);
    }

    @After
    public void tearDown() throws IOException {
        writer.close();
        DirectoryHelper.delete(directoryPath);
    }

    @Test
    public void shouldWriteOnInstanceViewChange() throws IOException {
        writer.changeInstanceView(1, 2);

        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put((byte) 1); // type
        buffer.putInt(1); // id
        buffer.putInt(2); // view

        assertArrayEquals(buffer.array(), readFile(directoryPath + "/sync.0.log"));
    }

    @Test
    public void shouldWriteOnInstanceValueChange() throws IOException {
        byte[] value = new byte[] {1, 2};
        writer.changeInstanceValue(1, 2, value);

        ByteBuffer buffer = ByteBuffer.allocate(15);
        buffer.put((byte) 2); // type
        buffer.putInt(1); // id
        buffer.putInt(2); // view
        buffer.putInt(2); // value size
        buffer.put(value); // value

        String path = directoryPath + "/sync.0.log";
        assertArrayEquals(readFile(path), buffer.array());
    }

    @Test
    public void shouldGetNextLogNumber() throws IOException {
        String[] s = new String[] {"sync.0.log", "invalid", "sync.2.log", "sync.1.log"};

        int lastLogNumber = writer.getLastLogNumber(s);
        assertEquals(lastLogNumber, 2);
    }

    @Test
    public void shouldChangeViewNumber() throws IOException {
        writer.changeViewNumber(5);

        DataInputStream stream = new DataInputStream(new FileInputStream(directoryPath +
                                                                         "/sync.0.view"));
        int view = stream.readInt();
        stream.close();
        assertEquals(view, 5);
    }

    @Test
    public void shouldLoad() throws IOException {
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
    public void shouldLoadCorruptedData() throws IOException {
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
    public void shouldLoadDataFromMoreFiles() throws IOException {
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
    public void shouldLoadViewNumber() throws IOException {
        writer.changeViewNumber(5);
        writer.close();

        writer = new FullSSDiscWriter(directoryPath);
        int view = writer.loadViewNumber();
        writer.close();

        assertEquals(view, 5);
    }

    @Test
    public void shouldWriteNewSnapshot() throws IOException {
        Snapshot snapshot = new Snapshot();

        snapshot.setNextInstanceId(2);
        snapshot.setValue(new byte[] {1, 2, 3});

        Map<Long, Reply> lastReplyForClient = new HashMap<Long, Reply>();
        lastReplyForClient.put((long) 1, new Reply(new RequestId(3, 1), new byte[] {1}));
        snapshot.setLastReplyForClient(lastReplyForClient);

        List<Reply> partialResponseCache = Arrays.asList(
                new Reply(new RequestId(1, 1), new byte[] {1, 2, 3}),
                new Reply(new RequestId(2, 2), new byte[] {1, 2, 3, 4}));
        snapshot.setPartialResponseCache(partialResponseCache);

        writer.newSnapshot(snapshot);
        writer.close();

        writer = new FullSSDiscWriter(directoryPath);
        writer.load();
        Snapshot actual = writer.getSnapshot();

        assertArrayEquals(snapshot.getValue(), actual.getValue());
        assertEquals(snapshot.getNextInstanceId(), actual.getNextInstanceId());
        assertEquals(snapshot.getNextRequestSeqNo(), actual.getNextRequestSeqNo());
        assertEquals(snapshot.getStartingRequestSeqNo(), actual.getStartingRequestSeqNo());
    }

    private byte[] readFile(String path) throws IOException {
        FileInputStream stream = new FileInputStream(path);
        int length = stream.available(); // danger
        byte[] value = new byte[length];
        stream.read(value);
        stream.close();
        return value;
    }
}
