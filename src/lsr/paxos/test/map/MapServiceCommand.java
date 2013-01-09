package lsr.paxos.test.map;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class MapServiceCommand implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Long key;
    private final Long value;

    public MapServiceCommand(Long key, Long value) {
        this.key = key;
        this.value = value;
    }

    public MapServiceCommand(byte[] bytes) throws IOException {
        DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(bytes));
        key = dataInput.readLong();
        value = dataInput.readLong();
    }

    public Long getKey() {
        return key;
    }

    public Long getValue() {
        return value;
    }

    public byte[] toByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(key);
        buffer.putLong(value);
        return buffer.array();
    }

    public String toString() {
        return String.format("[key=%d, value=%d]", key, value);
    }
}
