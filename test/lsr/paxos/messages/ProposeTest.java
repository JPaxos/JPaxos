package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class ProposeTest extends AbstractMessageTestCase<Propose> {
    private int view = 12;
    private int instanceId = 23;
    private byte[] value = new byte[] {1, 7, 4, 5};
    private Propose propose;

    @Before
    public void setUp() {
        propose = new Propose(view, instanceId, value);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(view, propose.getView());
        assertEquals(instanceId, propose.getInstanceId());
        assertTrue(Arrays.equals(value, propose.getValue()));
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(propose);

        byte[] bytes = propose.toByteArray();
        assertEquals(bytes.length, propose.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        Propose deserializedPropose = new Propose(dis);

        assertEquals(MessageType.Propose, type);
        compare(propose, deserializedPropose);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.Propose, propose.getType());
    }

    protected void compare(Propose expected, Propose actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getSentTime(), actual.getSentTime());
        assertEquals(expected.getType(), actual.getType());

        assertEquals(expected.getInstanceId(), actual.getInstanceId());
        assertTrue(Arrays.equals(expected.getValue(), actual.getValue()));
    }
}
