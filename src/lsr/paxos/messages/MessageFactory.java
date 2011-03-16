package lsr.paxos.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import lsr.common.Config;
import lsr.leader.messages.Ping;
import lsr.leader.messages.Pong;
import lsr.leader.messages.Report;
import lsr.leader.messages.SimpleAlive;
import lsr.leader.messages.Start;

/**
 * This class is responsible for serializing and deserializing messages to /
 * from byte array or input stream. The message has to be serialized using
 * <code>serialize()</code> method to deserialized it correctly.
 */
public class MessageFactory {

    /**
     * Creates a <code>Message</code> from serialized byte array.
     * 
     * @param message - serialized byte array with message content
     * @return deserialized message
     */
    public static Message readByteArray(byte[] message) {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(message));

        return create(input);
    }

    /**
     * Creates a <code>Message</code> from input stream.
     * 
     * @param input - the input stream with serialized message
     * @return deserialized message
     */
    public static Message create(DataInputStream input) {
        if (Config.JAVA_SERIALIZATION) {
            try {
                return (Message) (new ObjectInputStream(input).readObject());
            } catch (IOException e) {
                throw new IllegalArgumentException("Exception deserializing message occured!", e);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Exception deserializing message occured!", e);
            }
        }
        return createMine(input);
    }

    /**
     * Reads byte array and creates message from it. Byte array must have been
     * written by Message::toByteArray().
     * 
     * @param input - the input stream with serialized message inside
     * @return correct object from one of message subclasses
     * 
     * @throws IllegalArgumentException if a correct message could not be read
     *             from input
     */
    private static Message createMine(DataInputStream input) {
        MessageType type;
        Message message;

        try {
            type = MessageType.values()[input.readUnsignedByte()];
            message = createMessage(type, input);
        } catch (EOFException e) {
            throw new IllegalArgumentException(e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception deserializing message occured!", e);
        }

        return message;
    }

    /**
     * Serializes message to byte array.
     * 
     * @param message - the message to serialize
     * @return serialized message as byte array.
     */
    public static byte[] serialize(Message message) {
        byte[] data;
        if (Config.JAVA_SERIALIZATION) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                new ObjectOutputStream(baos).writeObject(message);
                data = baos.toByteArray();
            } catch (IOException e) {
                throw new IllegalArgumentException("Exception deserializing message occured!", e);
            }
        } else {
            data = message.toByteArray();
        }
        return data;
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
            case SimpleAlive:
                message = new SimpleAlive(input);
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
            case Nack:
                message = new Nack(input);
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
            case Ping:
                message = new Ping(input);
                break;
            case Pong:
                message = new Pong(input);
                break;
            case Report:
                message = new Report(input);
                break;
            case Start:
                message = new Start(input);
                break;
            case Recovery:
                message = new Recovery(input);
                break;
            case RecoveryAnswer:
                message = new RecoveryAnswer(input);
                break;
            default:
                throw new IllegalArgumentException("Unknown message type given to deserialize!");
        }
        return message;
    }
}
