package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class AliveTest extends AbstractMessageTestCase<Alive> {
    private int view = 12;
    private int logSize = 32;
    private Alive alive;

    @Before
    public void setUp() {
        alive = new Alive(view, logSize);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(view, alive.getView());
        assertEquals(logSize, alive.getLogSize());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(alive);

        byte[] bytes = alive.toByteArray();
        assertEquals(bytes.length, alive.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        Alive deserializedAlive = new Alive(dis);

        assertEquals(MessageType.Alive, type);

        compare(alive, deserializedAlive);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.Alive, alive.getType());
    }

    protected void compare(Alive expected, Alive actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());

        assertEquals(expected.getLogSize(), actual.getLogSize());
    }
}
