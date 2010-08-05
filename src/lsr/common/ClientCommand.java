package lsr.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Represents command which is sent by client to replica. In response to this
 * command, replica should send {@link ClientReply}.
 */
public class ClientCommand implements Serializable {
	private static final long serialVersionUID = 1L;
	private final CommandType _commandType;
	private final Request _request;

	/**
	 * The type of command.
	 */
	public enum CommandType {
		REQUEST, ALIVE
	};

	/**
	 * Creates new command.
	 * 
	 * @param commandType
	 *            type of command
	 * @param args
	 *            argument for this command
	 */
	public ClientCommand(CommandType commandType, Request args) {
		_commandType = commandType;
		_request = args;
	}

	public ClientCommand(DataInputStream input) throws IOException {

		_commandType = CommandType.values()[input.readInt()];

		byte[] args = new byte[input.readInt()];
		input.readFully(args);

		_request = Request.create(args);
	}

	public void writeToOutputStream(DataOutputStream stream) throws IOException {
		stream.writeInt(_commandType.ordinal());
		byte[] ba = _request.toByteArray();
		stream.writeInt(ba.length);
		stream.write(ba);
	}

	/**
	 * Returns the type of command.
	 * 
	 * @return command type
	 */
	public CommandType getCommandType() {
		return _commandType;
	}

	/**
	 * Returns the request (argument) for this command.
	 * 
	 * @return request (argument) object
	 */
	public Request getRequest() {
		return _request;
	}

	public String toString() {
		return _commandType + ": " + _request;
	}
}
