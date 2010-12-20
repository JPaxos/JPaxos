package lsr.paxos.messages;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

/**
 * Base test class for message tests.
 */
public abstract class AbstractMessageTestCase<T extends Message> {
    @SuppressWarnings("unchecked")
    protected void verifySerialization(T message) {
        byte[] bytes = MessageFactory.serialize(message);
        assertEquals(bytes.length, message.byteSize());

        T deserialized = (T) MessageFactory.readByteArray(bytes);
        compare(message, deserialized);

        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes));
        deserialized = (T) MessageFactory.create(stream);
        compare(message, deserialized);
    }

    protected abstract void compare(T first, T second);
}
