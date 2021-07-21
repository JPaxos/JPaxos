package lsr.common;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Represents command which is sent by client to replica. In response to this
 * command, replica should send {@link ClientReply}.
 */
public class ClientCommand implements Serializable {
    private static final long serialVersionUID = 1L;
    private final CommandType commandType;
    private final ClientRequest request;

    /**
     * The type of command.
     */
    public enum CommandType {
        REQUEST, ALIVE
    };

    /**
     * Creates new command.
     * 
     * @param commandType - the type of command
     * @param args - the argument for this command
     */
    public ClientCommand(CommandType commandType, ClientRequest args) {
        this.commandType = commandType;
        request = args;
    }

    /**
     * Creates new command from <code>ByteBuffer</code> which contain serialized
     * command.
     * 
     * @param input - the buffer with serialized command
     */
    public ClientCommand(ByteBuffer input) {
        commandType = CommandType.values()[input.getInt()];
        request = ClientRequest.create(input);
    }

    /**
     * Writes serialized command to specified buffer. The remaining amount of
     * bytes in the buffer has to be greater or equal than
     * <code>byteSize()</code>.
     * 
     * @param buffer - the byte buffer to write command to
     */
    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(commandType.ordinal());
        request.writeTo(buffer);
    }

    /**
     * The size of the command after serialization in bytes.
     * 
     * @return the size of the command in bytes
     */
    public int byteSize() {
        return 4 + request.byteSize();
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
    public ClientRequest getRequest() {
        return request;
    }

    public String toString() {
        return commandType + ": " + request;
    }

    /** Used to determine how many bytes must be read as header */
    public static final int HEADERS_SIZE = 4 + ClientRequest.HEADERS_SIZE;

    /** After how many bytes the size of value is stored */
    public static final int HEADER_VALUE_SIZE_OFFSET = 4 + ClientRequest.HEADER_VALUE_SIZE_OFFSET;

}
