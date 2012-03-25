package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class RecoveryTest extends AbstractMessageTestCase<Recovery> {
    private Recovery recovery;

    @Before
    public void setUp() {
        recovery = new Recovery(5, 10);
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.Recovery, recovery.getType());
    }

    @Test
    public void shoudReturnCorrectView() {
        assertEquals(5, recovery.getView());
    }

    @Test
    public void shouldReturnCorrectEpoch() {
        assertEquals(10, recovery.getEpoch());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(recovery);

        byte[] bytes = recovery.toByteArray();
        assertEquals(bytes.length, recovery.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        Recovery deserializedRecovery = new Recovery(dis);

        assertEquals(MessageType.Recovery, type);
        compare(recovery, deserializedRecovery);
        assertEquals(0, dis.available());
    }

    protected void compare(Recovery expected, Recovery actual) {
        assertEquals(expected.getView(), actual.getView());
        assertEquals(expected.getEpoch(), actual.getEpoch());
    }
}
