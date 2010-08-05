package lsr.paxos.replica;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Logger;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.KillOnExceptionHandler;

/**
 * Handles a connection from the client. Uses TCP. The connection is kept open
 * until the client closes it. Even if this process loses leadership, it keeps
 * the connection. Any request that may have been submitted at the time will
 * still be handled by this process.
 * 
 */
public class OldTcpClientProxy extends Thread implements ClientProxy {
	private static final int TIMEOUT = 10000;
	final Socket _socket;
	private DataInputStream _input;
	private DataOutputStream _output;
	private Long _clientId;
	private final CommandCallback _callback;
	private final ClientManager _clientManager;
	boolean _canceled;
	private final IdGenerator _idGenerator;

	/**
	 * Creates new connection to client.
	 * 
	 * @param socket
	 *            - active TCP socket connected to client
	 * @param callback
	 *            - command callback
	 * @param clientManager
	 *            - the manager where this client should register
	 * @param idGenerator
	 *            - generator of unique client id's
	 */
	public OldTcpClientProxy(Socket socket, CommandCallback callback, ClientManager clientManager,
			IdGenerator idGenerator) {
		super("ClientProxy");
		_callback = callback;
		_clientManager = clientManager;
		_idGenerator = idGenerator;
		_socket = socket;

		_canceled = false;
		setUncaughtExceptionHandler(new KillOnExceptionHandler());
		logger.fine("Socket created " + toString());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lsr.paxos.ClientProxy#send(lsr.common.ClientReply)
	 */
	public synchronized void send(ClientReply clientReply) throws IOException {
		if (!_socket.isClosed()) {
			logger.fine("Sending reply: " + clientReply);
			ByteArrayOutputStream prepare = new ByteArrayOutputStream();
			clientReply.write(new DataOutputStream(prepare));
			_output.write(prepare.toByteArray());
			_output.flush();
		}
	}

	public void run() {
		try {
			_socket.setSoTimeout(TIMEOUT);
			_socket.setTcpNoDelay(true);
			_socket.setSoLinger(true, 0);
		} catch (SocketException e) {
			throw new RuntimeException("Error while setting up sockets.", e);
		}
		try {
			_input = new DataInputStream(_socket.getInputStream());
			_output = new DataOutputStream(_socket.getOutputStream());
			initClientId();

			logger.info("Client connected: " + toString());
			_clientManager.addClient(_clientId, this);

			while (!_canceled) {
				ClientCommand command = new ClientCommand(_input);
				_callback.execute(command, this);
			}

		} catch (IOException ex) {
			logger.warning("Client disconnected (" + toString() + ").");
		} finally {
			cleanClose();
			if (_clientId != null)
				_clientManager.removeClient(_clientId);
		}
	}

	/**
	 * Stops all mechanism related with the underlying connection. It closes all
	 * sockets and streams, and also cancels the main loop waiting for new
	 * messages.
	 */
	public void close() {
		cleanClose();
		this.interrupt();
		_canceled = true;
	}

	/**
	 * Returns the closed state of the underlying socket.
	 * 
	 * @return <code>true</code> if the socket has been closed;
	 *         <code>false</code> otherwise
	 */
	public boolean isClosed() {
		return _socket.isClosed();
	}

	private void initClientId() throws IOException {
		// We get 'T' for true, 'F' for false
		boolean generateClientId = (_input.read() == 'F' ? false : true);
		if (generateClientId) {
			_clientId = _idGenerator.next();
			logger.info("Generated id for client: " + _clientId);
			_output.writeLong(_clientId);
			_output.flush();
		} else {
			_clientId = _input.readLong();
		}
	}

	void cleanClose() {
		// logger.fine("Socket closed " + toString());
		try {
			if (_socket != null)
				_socket.close();
		} catch (IOException e) {
			logger.warning("Not clean socket closing.");
		}
	}

	public String toString() {
		return String.format("Client: %s", _clientId);
	}

	private final static Logger logger = Logger.getLogger(OldTcpClientProxy.class.getCanonicalName());
}
