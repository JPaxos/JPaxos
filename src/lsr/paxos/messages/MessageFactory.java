package lsr.paxos.messages;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is responsible for serializing and deserializing messages to /
 * from byte array or input stream. The message has to be serialized using
 * <code>serialize()</code> method to deserialized it correctly.
 */
public final class MessageFactory {

    /**
     * Creates a <code>Message</code> from serialized byte array.
     * 
     * @param message - serialized byte array with message content
     * @return deserialized message
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static Message readByteArray(byte[] message) throws IOException {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(message));
        return create(input);
    }

    /**
     * Creates a <code>Message</code> from input stream. Reads byte array and
     * creates message from it. Byte array must have been written by
     * Message::toByteArray().
     * 
     * @param input - the input stream with serialized message inside
     * @return correct object from one of message subclasses
     * @throws IOException
     * 
     * @throws IllegalArgumentException if a correct message could not be read
     *             from input
     */
    public static Message create(DataInputStream input) throws IOException {
        MessageType type = MessageType.values()[input.readUnsignedByte()];
        Message message = createMessage(type, input);
        return message;
    }

    /**
     * Creates new message of specified type from given stream.
     * 
     * @param type - the type of message to create
     * @param input - the stream with serialized message
     * @return deserialized message
     * 
     * @throws IOException if I/O error occurs
     */
    private static Message createMessage(MessageType type, DataInputStream input)
            throws IOException {
        assert type != MessageType.ANY && type != MessageType.SENT : "Message type " + type +
                                                                     " cannot be serialized";

        Message message;
        switch (type) {
            case Accept:
                message = new Accept(input);
                break;
            case Alive:
                message = new Alive(input);
                break;
            case CatchUpQuery:
                message = new CatchUpQuery(input);
                break;
            case CatchUpResponse:
                message = new CatchUpResponse(input);
                break;
            case CatchUpSnapshot:
                message = new CatchUpSnapshot(input);
                break;
            case Prepare:
                message = new Prepare(input);
                break;
            case PrepareOK:
                message = new PrepareOK(input);
                break;
            case Propose:
                message = new Propose(input);
                break;
            case Recovery:
                message = new Recovery(input);
                break;
            case RecoveryAnswer:
                message = new RecoveryAnswer(input);
                break;
            case ForwardedClientBatch:
                message = new ForwardClientBatch(input);
                break;
            case AskForClientBatch:
                message = new AskForClientBatch(input);
                break;
            case ForwardedClientRequests:
                message = new ForwardClientRequests(input);
                break;
            default:
                throw new IllegalArgumentException("Unknown message type: " + type);
        }
        return message;
    }

    @SuppressWarnings("incomplete-switch")
    public static Message create(ByteBuffer bb) {
        byte typeOrd = bb.get();
        MessageType type = MessageType.values()[typeOrd];

        switch (type) {
            case Accept:
                return new Accept(bb);
            case Alive:
                return new Alive(bb);
            case CatchUpQuery:
                return new CatchUpQuery(bb);
            case CatchUpResponse:
                return new CatchUpResponse(bb);
            case CatchUpSnapshot:
                return new CatchUpSnapshot(bb);
            case Prepare:
                return new Prepare(bb);
            case PrepareOK:
                return new PrepareOK(bb);
            case Propose:
                return new Propose(bb);
            case Recovery:
                return new Recovery(bb);
            case RecoveryAnswer:
                return new RecoveryAnswer(bb);
            case ForwardedClientBatch:
                return new ForwardClientBatch(bb);
            case AskForClientBatch:
                return new AskForClientBatch(bb);
            case ForwardedClientRequests:
                return new ForwardClientRequests(bb);
        }
        throw new IllegalArgumentException("Unknown message type: " + type);
    }
}
