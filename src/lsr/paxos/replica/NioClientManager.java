package lsr.paxos.replica;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import lsr.common.nio.AcceptHandler;
import lsr.common.nio.ReaderAndWriter;
import lsr.common.nio.SelectorThread;

/**
 * This class is responsible for accepting new connections from the client. It
 * uses java nio package, so it is possible to handle more client connections.
 * Every client connection is then handled by new <code>NioClientProxy</code>
 * instance. To start waiting for client connection, start method has to be
 * invoked.
 * 
 * @see NioClientProxy
 */
public class NioClientManager implements AcceptHandler {
	private final SelectorThread _selectorThread;
	private final int _localPort;
	private final IdGenerator _idGenerator;
	private final CommandCallback _callback;
	private ServerSocketChannel _serverSocketChannel;

	/**
	 * Creates new client manager.
	 * 
	 * @param localPort
	 *            - the listen port for client connections
	 * @param commandCallback
	 *            - callback invoked every time new message is received by
	 *            client
	 * @param idGenerator
	 *            - generator used to allocate id's for clients
	 * @throws IOException
	 *             - if creating selector failed
	 */
	public NioClientManager(int localPort, CommandCallback commandCallback,
			IdGenerator idGenerator) throws IOException {
		_localPort = localPort;
		_callback = commandCallback;
		_idGenerator = idGenerator;
		_selectorThread = new SelectorThread();
	}

	/**
	 * Starts listening and handling client connections.
	 * 
	 * @throws IOException
	 *             - if error occurs while preparing socket channel
	 */
	public void start() throws IOException {
		_serverSocketChannel = ServerSocketChannel.open();
		InetSocketAddress address = new InetSocketAddress(_localPort);
		_serverSocketChannel.socket().bind(address);

		_selectorThread.scheduleRegisterChannel(_serverSocketChannel,
				SelectionKey.OP_ACCEPT, this);

		_selectorThread.start();
	}

	/**
	 * This method is called by <code>SelectorThread</code> every time new
	 * connection from client can be accepted. After accepting, new
	 * <code>NioClientProxy</code> is created to handle this connection.
	 */
	public void handleAccept() {
		SocketChannel socketChannel = null;
		try {
			socketChannel = _serverSocketChannel.accept();
		} catch (IOException e) {
			// TODO: probably to many open files exception,
			// but i don't know what to do then; is server socket channel valid
			// after throwing this exception?; if yes can we just ignore it and
			// wait for new connections?
			throw new RuntimeException(e);
		}

		_selectorThread.addChannelInterest(_serverSocketChannel,
				SelectionKey.OP_ACCEPT);

		// if accepting was successful create new client proxy
		if (socketChannel != null) {
			try {
				ReaderAndWriter raw = new ReaderAndWriter(socketChannel,
						_selectorThread);
				new NioClientProxy(raw, _callback, _idGenerator);
			} catch (IOException e) {
				// TODO: probably registering to selector has failed; should we
				// just close the client connection?
				e.printStackTrace();
			}
			_logger.info("Connection established");
		}
	}

	private final static Logger _logger = Logger
			.getLogger(NioClientManager.class.getCanonicalName());
}
