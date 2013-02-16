package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import lsr.paxos.storage.ConsensusInstance;

/**
 * Represents the <code>Propose</code> message sent by leader to vote on next
 * consensus instance. As every message it contains the view number of sender
 * process and additionally the id of new consensus instance and its value as
 * byte array.
 */
public class Propose extends Message {
    private static final long serialVersionUID = 1L;
    private final byte[] value;
    private final int instanceId;

    /**
     * Creates new <code>Propose</code> message to propose specified instance ID
     * and value.
     * 
     * @param view - sender view number
     * @param instanceId - the ID of instance to propose
     * @param value - the value of the instance
     */
    public Propose(int view, int instanceId, byte[] value) {
        super(view);
        assert value != null;
        this.instanceId = instanceId;
        this.value = value;
        assert this.value != null;
    }

    /**
     * Creates new <code>Propose</code> message from consensus instance. The ID
     * and the value is taken from this object.
     * 
     * @param instance - the consensus instance to propose
     */
    public Propose(ConsensusInstance instance) {
        super(instance.getView());
        instanceId = instance.getId();
        value = instance.getValue();
        assert value != null;
    }

    /**
     * Creates new <code>Propose</code> message from serialized input stream.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs while deserializing
     */
    public Propose(DataInputStream input) throws IOException {
        super(input);

        instanceId = input.readInt();
        value = new byte[input.readInt()];
        input.readFully(value);
    }

    /**
     * Returns the ID of proposed instance.
     * 
     * @return the ID of proposed instance
     */
    public int getInstanceId() {
        return instanceId;
    }

    /**
     * Returns value of proposed instance.
     * 
     * @return value of proposed instance
     */
    public byte[] getValue() {
        return value;
    }

    public MessageType getType() {
        return MessageType.Propose;
    }

    public int byteSize() {
        return super.byteSize() + 4 + 4 + value.length;
    }

    public String toString() {
        return "Propose(" + super.toString() + ", i:" + getInstanceId() + ")";
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(instanceId);
        bb.putInt(value.length);
        bb.put(value);
    }
}
