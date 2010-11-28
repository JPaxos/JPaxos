package lsr.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Represents command which is sent by client to replica. In response to this
 * command, replica should send {@link ClientReply}.
 */
public class ClientCommand implements Serializable {
    private static final long serialVersionUID = 1L;
    private final CommandType commandType;
    private final Request request;

    /**
     * The type of command.
     */
    public enum CommandType {
        REQUEST, ALIVE
    };

    /**
     * Creates new command.
     * 
     * @param commandType type of command
     * @param args argument for this command
     */
    public ClientCommand(CommandType commandType, Request args) {
        this.commandType = commandType;
        request = args;
    }

    /**
     * @deprecated Use {@link #ClientCommand(ByteBuffer)}
     * @param input
     * @throws IOException
     */
    public ClientCommand(DataInputStream input) throws IOException {

        commandType = CommandType.values()[input.readInt()];

        byte[] args = new byte[input.readInt()];
        input.readFully(args);

        request = Request.create(args);
    }

    public ClientCommand(ByteBuffer input) throws IOException {
        commandType = CommandType.values()[input.getInt()];
        // Discard the next int, size of request.
        input.getInt();
        request = Request.create(input);
    }

    /**
     * @deprecated Use {@link #writeToByteBuffer(ByteBuffer)}
     * @param stream
     * @throws IOException
     */
    public void writeToOutputStream(DataOutputStream stream) throws IOException {
        stream.writeInt(commandType.ordinal());
        byte[] ba = request.toByteArray();
        stream.writeInt(ba.length);
        stream.write(ba);
    }

    public void writeToByteBuffer(ByteBuffer buffer) throws IOException {
        buffer.putInt(commandType.ordinal());
        buffer.putInt(request.byteSize());
        request.writeTo(buffer);
    }

    public int byteSize() {
        return 4 + 4 + request.byteSize();
    }

    /**
     * Returns the type of command.
     * 
     * @return command type
     */
    public CommandType getCommandType() {
        return commandType;
    }

    /**
     * Returns the request (argument) for this command.
     * 
     * @return request (argument) object
     */
    public Request getRequest() {
        return request;
    }

    public String toString() {
        return commandType + ": " + request;
    }
}
