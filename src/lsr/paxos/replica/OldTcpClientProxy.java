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
	final Socket socket;
	private DataInputStream input;
	private DataOutputStream output;
	private Long clientId;
	private final CommandCallback callback;
	private final ClientManager clientManager;
	boolean canceled;
	private final IdGenerator idGenerator;

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
	public OldTcpClientProxy(Socket socket, CommandCallback callback,
			ClientManager clientManager, IdGenerator idGenerator) {
		super("ClientProxy");
		this.callback = callback;
		this.clientManager = clientManager;
		this.idGenerator = idGenerator;
		this.socket = socket;

		canceled = false;
		setUncaughtExceptionHandler(new KillOnExceptionHandler());
		logger.fine("Socket created " + toString());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lsr.paxos.ClientProxy#send(lsr.common.ClientReply)
	 */
	public synchronized void send(ClientReply clientReply) throws IOException {
		if (!socket.isClosed()) {
			logger.fine("Sending reply: " + clientReply);
			ByteArrayOutputStream prepare = new ByteArrayOutputStream();
			clientReply.write(new DataOutputStream(prepare));
			output.write(prepare.toByteArray());
			output.flush();
		}
	}

	public void run() {
		try {
			socket.setSoTimeout(TIMEOUT);
			socket.setTcpNoDelay(true);
			socket.setSoLinger(true, 0);
		} catch (SocketException e) {
			throw new RuntimeException("Error while setting up sockets.", e);
		}
		try {
			input = new DataInputStream(socket.getInputStream());
			output = new DataOutputStream(socket.getOutputStream());
			initClientId();

			logger.info("Client connected: " + toString());
			clientManager.addClient(clientId, this);

			while (!canceled) {
				ClientCommand command = new ClientCommand(input);
				callback.execute(command, this);
			}

		} catch (IOException ex) {
			logger.warning("Client disconnected (" + toString() + ").");
		} finally {
			cleanClose();
			if (clientId != null)
				clientManager.removeClient(clientId);
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
		canceled = true;
	}

	/**
	 * Returns the closed state of the underlying socket.
	 * 
	 * @return <code>true</code> if the socket has been closed;
	 *         <code>false</code> otherwise
	 */
	public boolean isClosed() {
		return socket.isClosed();
	}

	private void initClientId() throws IOException {
		// We get 'T' for true, 'F' for false
		boolean generateClientId = (input.read() == 'F' ? false : true);
		if (generateClientId) {
			clientId = idGenerator.next();
			logger.info("Generated id for client: " + clientId);
			output.writeLong(clientId);
			output.flush();
		} else {
			clientId = input.readLong();
		}
	}

	void cleanClose() {
		// logger.fine("Socket closed " + toString());
		try {
			if (socket != null)
				socket.close();
		} catch (IOException e) {
			logger.warning("Not clean socket closing.");
		}
	}

	public String toString() {
		return String.format("Client: %s", clientId);
	}

	private final static Logger logger = Logger
			.getLogger(OldTcpClientProxy.class.getCanonicalName());
}
