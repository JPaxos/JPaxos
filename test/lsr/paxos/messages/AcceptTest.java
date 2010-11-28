package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class AcceptTest {
    private Accept accept;
    private int view;
    private int instanceId;
    private byte[] values;

    @Before
    public void setUp() {
        view = 123;
        instanceId = 432;
        values = new byte[] {1, 5, 7, 3};
        accept = new Accept(view, instanceId);
    }

    @Test
    public void testDefaultConstructor() {
        assertEquals(view, accept.getView());
        assertEquals(instanceId, accept.getInstanceId());
    }

    @Test
    public void testAcceptFromProposeMessage() {
        Propose propose = new Propose(view, instanceId, values);
        Accept accept = new Accept(propose);
        assertEquals(accept, accept);
    }

    @Test
    public void testSerialization() throws IOException {
        byte[] bytes = accept.toByteArray();

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        Accept deserializedAccept = new Accept(dis);

        assertEquals(MessageType.Accept, type);
        assertEquals(accept, deserializedAccept);
        assertEquals(0, dis.available());
        assertEquals(bytes.length, accept.byteSize());
    }

    @Test
    public void testCorrectMessageType() {
        assertEquals(MessageType.Accept, accept.getType());
    }
}
