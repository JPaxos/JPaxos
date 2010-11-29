package lsr.paxos.replica;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Map;
import java.util.logging.Logger;

import lsr.common.KillOnExceptionHandler;

/**
 * Handles all TCP connection from the clients. It accepts new connections from
 * client, and create the new client proxy.
 * 
 * @deprecated Use {@link NioClientManager} 
 */
public class TcpClientManager extends Thread {
    private final Object lock = new Object();
    private final int clientPort;
    private final Map<Long, TcpClientProxy> clients = new Hashtable<Long, TcpClientProxy>();
    private final CommandCallback callback;
    private final IdGenerator idGenerator;

    /**
     * Creates a new <code>ClientManager</code>.
     * 
     * @param clientPort - the port on which clients will connect
     * @param callback - callback called every time new command is received from
     *            client
     * @param idGenerator - for generating new id's for new clients
     */
    public TcpClientManager(int clientPort, CommandCallback callback, IdGenerator idGenerator) {
        super("ClientManager");
        setDefaultUncaughtExceptionHandler(new KillOnExceptionHandler());
        this.clientPort = clientPort;
        this.callback = callback;
        this.idGenerator = idGenerator;
    }

    public void run() {
        try {
            receiveConnections();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts listening and accepting connections from clients. When connection
     * is successful, new <code>ClientProxy</code> is created.
     * 
     * @throws IOException if an I/O error occurs when opening the socket or
     *             when waiting for a connection
     */
    private void receiveConnections() throws IOException {
        ServerSocket ss = new ServerSocket(clientPort);
        ss.setReuseAddress(true);
        while (true) {
            Socket socket = ss.accept();
            TcpClientProxy client = new TcpClientProxy(socket, callback, this, idGenerator);
            client.start();
        }
    }

    /**
     * Unregisters client with specified id from list of active connections.
     * 
     * @param clientId - id of client
     */
    public void removeClient(Long clientId) {
        logger.fine("Removing client: " + clientId);
        synchronized (lock) {
            clients.remove(clientId);
        }
    }

    /**
     * Registers new active client connection. If there exists old connection to
     * this client, it is closed.
     * 
     * @param clientId - the id of client
     * @param client - client connection
     */
    public void addClient(long clientId, TcpClientProxy client) {
        TcpClientProxy oldClient = clients.get(clientId);
        if (oldClient != null) {
            logger.severe("Client connected again (old connection exists):" + clientId);
            oldClient.close();
        }
        synchronized (lock) {
            clients.put(new Long(clientId), client);
        }
    }

    private final static Logger logger = Logger.getLogger(TcpClientManager.class.getCanonicalName());
}
