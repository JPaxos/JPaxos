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
    private final SelectorThread selectorThread;
    private final int localPort;
    private final IdGenerator idGenerator;
    private final CommandCallback callback;
    private ServerSocketChannel serverSocketChannel;

    /**
     * Creates new client manager.
     * 
     * @param localPort - the listen port for client connections
     * @param commandCallback - callback invoked every time new message is
     *            received by client
     * @param idGenerator - generator used to allocate id's for clients
     * @throws IOException - if creating selector failed
     */
    public NioClientManager(int localPort, CommandCallback commandCallback, IdGenerator idGenerator)
            throws IOException {
        this.localPort = localPort;
        this.callback = commandCallback;
        this.idGenerator = idGenerator;
        selectorThread = new SelectorThread();
    }

    /**
     * Starts listening and handling client connections.
     * 
     * @throws IOException - if error occurs while preparing socket channel
     */
    public void start() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(localPort);
        serverSocketChannel.socket().bind(address);

        selectorThread.scheduleRegisterChannel(serverSocketChannel, SelectionKey.OP_ACCEPT, this);

        selectorThread.start();
    }

    /**
     * This method is called by <code>SelectorThread</code> every time new
     * connection from client can be accepted. After accepting, new
     * <code>NioClientProxy</code> is created to handle this connection.
     */
    public void handleAccept() {
        SocketChannel socketChannel = null;
        try {
            socketChannel = serverSocketChannel.accept();
        } catch (IOException e) {
            // TODO: probably to many open files exception,
            // but i don't know what to do then; is server socket channel valid
            // after throwing this exception?; if yes can we just ignore it and
            // wait for new connections?
            throw new RuntimeException(e);
        }

        selectorThread.addChannelInterest(serverSocketChannel, SelectionKey.OP_ACCEPT);

        // if accepting was successful create new client proxy
        if (socketChannel != null) {
            try {
                ReaderAndWriter raw = new ReaderAndWriter(socketChannel, selectorThread);
                new NioClientProxy(raw, callback, idGenerator);
            } catch (IOException e) {
                // TODO: probably registering to selector has failed; should we
                // just close the client connection?
                e.printStackTrace();
            }
            _logger.info("Connection established");
        }
    }

    private final static Logger _logger = Logger.getLogger(NioClientManager.class.getCanonicalName());
}
