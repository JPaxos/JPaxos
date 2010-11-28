package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import lsr.paxos.storage.ConsensusInstance;

import org.junit.Before;
import org.junit.Test;

public class PrepareOKTest {
    private int view = 12;
    private PrepareOK prepareOK;
    private ConsensusInstance[] instances;

    @Before
    public void setUp() {
        instances = new ConsensusInstance[3];
        instances[0] = new ConsensusInstance(0);
        instances[0].setValue(4, new byte[] {1, 2, 3});
        instances[1] = new ConsensusInstance(1);
        instances[0].setValue(5, new byte[] {1, 4, 3});
        instances[2] = new ConsensusInstance(2);
        instances[0].setValue(6, new byte[] {6, 9, 2});

        prepareOK = new PrepareOK(view, instances);
    }

    @Test
    public void testDefaultConstructor() {
        assertEquals(view, prepareOK.getView());
        assertTrue(Arrays.equals(instances, prepareOK.getPrepared()));
    }

    @Test
    public void testSerialization() throws IOException {
        byte[] bytes = prepareOK.toByteArray();
        assertEquals(bytes.length, prepareOK.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        PrepareOK deserializedPrepare = new PrepareOK(dis);

        assertEquals(MessageType.PrepareOK, type);
        compare(prepareOK, deserializedPrepare);
        assertEquals(0, dis.available());
    }

    @Test
    public void testCorrectMessageType() {
        assertEquals(MessageType.PrepareOK, prepareOK.getType());
    }

    private void compare(PrepareOK expected, PrepareOK actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());

        assertTrue(Arrays.equals(expected.getPrepared(), actual.getPrepared()));
    }
}
