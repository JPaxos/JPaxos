package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class PrepareTest {
    private int view = 12;
    private int firstUncommitted = 32;
    private Prepare prepare;

    @Before
    public void setUp() {
        prepare = new Prepare(view, firstUncommitted);
    }

    @Test
    public void testDefaultConstructor() {
        assertEquals(view, prepare.getView());
        assertEquals(firstUncommitted, prepare.getFirstUncommitted());
    }

    @Test
    public void testSerialization() throws IOException {
        byte[] bytes = prepare.toByteArray();
        assertEquals(bytes.length, prepare.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        Prepare deserializedPrepare = new Prepare(dis);

        assertEquals(MessageType.Prepare, type);
        compare(prepare, deserializedPrepare);
        assertEquals(0, dis.available());
    }

    @Test
    public void testCorrectMessageType() {
        assertEquals(MessageType.Prepare, prepare.getType());
    }

    private void compare(Prepare expected, Prepare actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());

        assertEquals(expected.getFirstUncommitted(), actual.getFirstUncommitted());
    }
}
