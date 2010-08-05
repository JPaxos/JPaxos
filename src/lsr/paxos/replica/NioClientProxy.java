package lsr.paxos.replica;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.Config;
import lsr.common.nio.PacketHandler;
import lsr.common.nio.ReaderAndWriter;

/**
 * This class is used to handle one client connection. It uses
 * <code>ReaderAndWriter</code> for writing and reading packets from clients.
 * <p>
 * First it initializes client connection reading client id (or granting him a
 * new one). After successful initialization commands will be received, and
 * reply can be send to clients.
 * 
 * @see ReaderAndWriter
 */
public class NioClientProxy implements ClientProxy {
	private final CommandCallback _callback;
	private boolean _initialized = false;
	private long _clientId;
	private final IdGenerator _idGenerator;
	private final ByteBuffer _readBuffer = ByteBuffer.allocate(1024);
	private final ReaderAndWriter _readerAndWriter;

	/**
	 * Creates new client proxy.
	 * 
	 * @param readerAndWriter
	 *            - used to send and receive data from clients
	 * @param callback
	 *            - callback for executing command from clients
	 * @param idGenerator
	 *            - generator used to generate id's for clients
	 */
	public NioClientProxy(ReaderAndWriter readerAndWriter,
			CommandCallback callback, IdGenerator idGenerator) {
		_readerAndWriter = readerAndWriter;
		_callback = callback;
		_idGenerator = idGenerator;

		_logger.info("New client connection: "
				+ readerAndWriter._socketChannel.socket());
		_readerAndWriter.setPacketHandler(new InitializePacketHandler(
				_readBuffer));
	}

	/**
	 * Sends the reply to client holding by this proxy. This method has to be
	 * called after client is initialized.
	 */
	public void send(ClientReply clientReply) throws IOException {
		if (!_initialized)
			throw new IllegalStateException("Connection not initialized yet");

		if (Config.javaSerialization) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			(new ObjectOutputStream(baos)).writeObject(clientReply);
			_readerAndWriter.send(baos.toByteArray());
		} else {
			_readerAndWriter.send(clientReply.toByteArray());
		}
	}

	/** executes command from byte buffer */
	private void execute(ByteBuffer buffer) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array());

			ClientCommand command;
			if (Config.javaSerialization)
				command = (ClientCommand) (new ObjectInputStream(bais))
						.readObject();
			else
				command = new ClientCommand(new DataInputStream(bais));

			_callback.execute(command, this);
		} catch (IOException e) {
			// command client is incorrect; close the underlying connection
			_logger.log(Level.WARNING,
					"Client command is incorrect. Closing channel.", e);
			_readerAndWriter.close();
		} catch (ClassNotFoundException e) {
			// command client is incorrect; close the underlying connection
			_logger.log(Level.WARNING,
					"Client command is incorrect. Closing channel.", e);
			_readerAndWriter.close();
		}
	}

	/**
	 * Waits for first byte, 'T' or 'F' which specifies whether we should grant
	 * new id for this client, or it has one already.
	 */
	private class InitializePacketHandler implements PacketHandler {
		private final ByteBuffer _buffer;

		public InitializePacketHandler(ByteBuffer buffer) {
			_buffer = buffer;
			_buffer.clear();
			_buffer.limit(1);
		}

		public void finished() {
			_buffer.rewind();
			byte b = _buffer.get();

			if (b == 'T') {
				// grant new id for client
				_clientId = _idGenerator.next();
				byte[] bytesClientId = new byte[8];
				ByteBuffer.wrap(bytesClientId).putLong(_clientId);
				_readerAndWriter.send(bytesClientId);

				if (Config.javaSerialization)
					_readerAndWriter
							.setPacketHandler(new UniversalClientCommandPacketHandler(
									_buffer));
				else
					_readerAndWriter
							.setPacketHandler(new MyClientCommandPacketHandler(
									_buffer));

				_initialized = true;
			} else if (b == 'F') {
				// wait for receiving id from client
				_readerAndWriter.setPacketHandler(new ClientIdPacketHandler(
						_buffer));
			} else {
				// command client is incorrect; close the underlying connection
				_logger.log(Level.WARNING,
						"Incorrect initialization header. Expected 'T' or 'F but received "
								+ b);
				_readerAndWriter.close();
			}
		}

		public ByteBuffer getByteBuffer() {
			return _buffer;
		}
	}

	/**
	 * Waits for the id from client. After receiving it starts receiving client
	 * commands packets.
	 */
	private class ClientIdPacketHandler implements PacketHandler {
		private final ByteBuffer _buffer;

		public ClientIdPacketHandler(ByteBuffer buffer) {
			_buffer = buffer;
			_buffer.clear();
			_buffer.limit(8);
			_initialized = true;
		}

		public void finished() {
			_buffer.rewind();
			_clientId = _buffer.getLong();
			if (Config.javaSerialization)
				_readerAndWriter
						.setPacketHandler(new UniversalClientCommandPacketHandler(
								_buffer));
			else
				_readerAndWriter
						.setPacketHandler(new MyClientCommandPacketHandler(
								_buffer));
		}

		public ByteBuffer getByteBuffer() {
			return _buffer;
		}
	}

	/**
	 * Waits for the header and then for the message from the client.
	 */
	private class MyClientCommandPacketHandler implements PacketHandler {
		private final ByteBuffer _defaultBuffer;
		private ByteBuffer _buffer;
		private boolean header = true;

		public MyClientCommandPacketHandler(ByteBuffer buffer) {
			_defaultBuffer = buffer;
			_buffer = _defaultBuffer;
			_buffer.clear();
			_buffer.limit(8);
		}

		public void finished() {
			if (header) {
				assert _buffer == _defaultBuffer : "Default buffer should be used for reading header";

				_defaultBuffer.rewind();
				int firstNumber = _defaultBuffer.getInt();
				int sizeOfValue = _defaultBuffer.getInt();
				if (8 + sizeOfValue <= _defaultBuffer.capacity()) {
					_defaultBuffer.limit(8 + sizeOfValue);
				} else {
					_buffer = ByteBuffer.allocate(8 + sizeOfValue);
					_buffer.putInt(firstNumber);
					_buffer.putInt(sizeOfValue);
				}
			} else {
				execute(_buffer);
				// for reading header we can use default buffer
				_buffer = _defaultBuffer;
				_defaultBuffer.clear();
				_defaultBuffer.limit(8);
			}
			header = !header;
			_readerAndWriter.setPacketHandler(this);
		}

		public ByteBuffer getByteBuffer() {
			return _buffer;
		}
	}

	/**
	 * Assumes that first received integer represent the size of value.
	 */
	private class UniversalClientCommandPacketHandler implements PacketHandler {
		private final ByteBuffer _defaultBuffer;
		private ByteBuffer _buffer;
		private boolean readSize = true;

		public UniversalClientCommandPacketHandler(ByteBuffer buffer) {
			_defaultBuffer = buffer;
			_buffer = _defaultBuffer;
			_buffer.clear();
			_buffer.limit(/* sizeof(int) */4);
		}

		public void finished() {
			if (readSize) {
				assert _buffer == _defaultBuffer : "Default buffer should be used for reading header";

				_defaultBuffer.rewind();
				int size = _defaultBuffer.getInt();
				if (size <= _defaultBuffer.capacity()) {
					_defaultBuffer.limit(size);
				} else {
					_buffer = ByteBuffer.allocate(size);
				}
				_defaultBuffer.rewind();
			} else {
				execute(_buffer);
				// for reading header we can use default buffer
				_buffer = _defaultBuffer;
				_defaultBuffer.clear();
				_defaultBuffer.limit(4);
			}
			readSize = !readSize;
			_readerAndWriter.setPacketHandler(this);
		}

		public ByteBuffer getByteBuffer() {
			return _buffer;
		}
	}

	@Override
	public String toString() {
		// return String.format("Client: %s", _clientId);
		return "client: " + _clientId;
	}

	private final static Logger _logger = Logger.getLogger(NioClientProxy.class
			.getCanonicalName());
}
