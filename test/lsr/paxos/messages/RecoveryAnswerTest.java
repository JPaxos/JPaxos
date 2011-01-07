package lsr.paxos.messages;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class RecoveryAnswerTest extends AbstractMessageTestCase<RecoveryAnswer> {
    private RecoveryAnswer recoveryAnswer;

    @Before
    public void setUp() {
        recoveryAnswer = new RecoveryAnswer(5, new long[] {1, 2, 3}, 10);
    }

    @Test
    public void shouldInitializeUsingTwoArgumentConstructor() {
        recoveryAnswer = new RecoveryAnswer(5, 10);
        assertEquals(MessageType.RecoveryAnswer, recoveryAnswer.getType());
        assertEquals(5, recoveryAnswer.getView());
        assertEquals(10, recoveryAnswer.getNextId());
        assertArrayEquals(new long[0], recoveryAnswer.getEpoch());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.RecoveryAnswer, recoveryAnswer.getType());
    }

    @Test
    public void shoudReturnCorrectView() {
        assertEquals(5, recoveryAnswer.getView());
    }

    @Test
    public void shouldReturnCorrectEpoch() {
        assertArrayEquals(new long[] {1, 2, 3}, recoveryAnswer.getEpoch());
    }

    @Test
    public void shouldReturnCorrectNextId() {
        assertEquals(10, recoveryAnswer.getNextId());
    }

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {
        verifySerialization(recoveryAnswer);

        byte[] bytes = recoveryAnswer.toByteArray();
        assertEquals(bytes.length, recoveryAnswer.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        RecoveryAnswer deserializedRecovery = new RecoveryAnswer(dis);

        assertEquals(MessageType.RecoveryAnswer, type);
        compare(recoveryAnswer, deserializedRecovery);
        assertEquals(0, dis.available());
    }

    protected void compare(RecoveryAnswer expected, RecoveryAnswer actual) {
        assertEquals(expected.getView(), actual.getView());
        assertArrayEquals(expected.getEpoch(), actual.getEpoch());
    }
}
