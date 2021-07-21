package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents the <code>Accept</code> message. This message is sent as a
 * response to <code>Propose</code> message from the leader.
 */
public class Accept extends Message {
    private static final long serialVersionUID = 1L;
    private final int instanceId;

    /**
     * Creates new <code>Accept</code> message as a response to
     * <code>Propose</code> message.
     * 
     * @param message - the propose message
     */
    public Accept(Propose message) {
        super(message.getView());
        instanceId = message.getInstanceId();
    }

    /**
     * Creates new <code>Accept</code> message with specified view and instance
     * id.
     * 
     * @param view - the view number
     * @param instanceId - the instance id
     */
    public Accept(int view, int instanceId) {
        super(view);
        this.instanceId = instanceId;
    }

    /**
     * Creates new <code>Accept</code> message from input stream with serialized
     * message.
     * 
     * @param input - input stream with serialized <code>Accept</code> message
     *            inside.
     * @throws IOException if I/O error occurs when deserializing
     */
    public Accept(DataInputStream input) throws IOException {
        super(input);
        instanceId = input.readInt();
    }

    public Accept(ByteBuffer bb) {
        super(bb);
        instanceId = bb.getInt();
    }

    /**
     * Returns the instance id.
     * 
     * @return the instance id
     */
    public int getInstanceId() {
        return instanceId;
    }

    public MessageType getType() {
        return MessageType.Accept;
    }

    public int byteSize() {
        return super.byteSize() + 4;
    }

    public String toString() {
        return "Accept(" + super.toString() + ", i:" + getInstanceId() + ")";
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(instanceId);
    }
}
