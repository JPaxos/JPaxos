package lsr.paxos.messages;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

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
    public static Message readByteArray(byte[] message) throws IOException, ClassNotFoundException {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(message));
        return create(input);
    }

    /**
     * Creates a <code>Message</code> from input stream.
     * Reads byte array and creates message from it. Byte array must have been
     * written by Message::toByteArray().
     * 
     * @param input - the input stream with serialized message inside
     * @return correct object from one of message subclasses
     * @throws IOException 
     * 
     * @throws IllegalArgumentException if a correct message could not be read
     *             from input
     */
    public static Message create(DataInputStream input) throws IOException, ClassNotFoundException {
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
            case BatchCatchUpQuery:
                message = new BatchCatchUpQuery(input);
                break;
			case BatchCatchUpResponse:
                message = new BatchCatchUpResponse(input);
                break;
			case CatchUpQuery:
                message = new CatchUpQuery(input);
                break;
            case CatchUpResponse:
                message = new CatchUpResponse(input);
                break;
         /*   case CatchUpSnapshot:
                message = new CatchUpSnapshot(input);
                break;*/
            case InstanceCatchUpQuery:
                message = new InstanceCatchUpQuery(input);
                break;
			case InstanceCatchUpResponse:
                message = new InstanceCatchUpResponse(input);
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
			case RecoveryQuery:
                message = new RecoveryQuery(input);
                break;
			case RecoveryResponse:
                message = new RecoveryResponse(input);
                break;
            case ForwardedClientRequest:
                message = new ForwardClientBatch(input);
                break;
            case ViewPrepared:
                message = new ViewPrepared(input);
                break;
            case AckForwardedRequest:
                message = new AckForwardClientBatch(input);
                break;            
            default:
                throw new IllegalArgumentException("Unknown message type: " + type);
        }
        return message;
    }
}
