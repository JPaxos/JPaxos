package lsr.paxos.replica;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Config;
import lsr.common.ProcessDescriptor;
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
    private final SelectorThread[] selectorThreads;
    private int nextThread=0;
    private final int localPort;
    private final IdGenerator idGenerator;
    private final RequestManager requestManager;
    private ServerSocketChannel serverSocketChannel;
    private volatile boolean started = false;

    /**
     * Creates new client manager.
     * 
     * @param localPort - the listen port for client connections
     * @param requestManager - callback invoked every time new message is
     *            received by client
     * @param idGenerator - generator used to allocate id's for clients
     * @throws IOException if creating selector failed
     */
    public NioClientManager(int localPort, RequestManager requestManager, IdGenerator idGenerator)
            throws IOException {
        this.localPort = localPort;
        this.requestManager = requestManager;
        this.idGenerator = idGenerator;

        int nSelectors=ProcessDescriptor.getInstance().selectorThreads;
        if (nSelectors == -1) {
            nSelectors = NioClientManager.computeNSelectors();            
        } else {
            if (nSelectors < 0) {
                throw new IOException("Invalid value for property " + Config.SELECTOR_THREADS + ": " + nSelectors);
            }
            logger.info("Number of selector threads: " + nSelectors);
        }
        selectorThreads = new SelectorThread[nSelectors];
        for (int i = 0; i < selectorThreads.length; i++) {
            selectorThreads[i] = new SelectorThread(i);
        }
        
        requestManager.setNioClientManager(this);
    }
    
    public void executeInAllSelectors(Runnable r) {
        for (SelectorThread  sThread : selectorThreads) {
            sThread.beginInvoke(r);
        }
    }

    private static int computeNSelectors() {
        int nProcessors = Runtime.getRuntime().availableProcessors();
        int n;
        // Values determined empirically based on tests on 24 core Opteron system.
        if (nProcessors < 3) {
            n=1;
        } else if (nProcessors < 5) {
            n=2;
        } else if (nProcessors < 7) {
            n=3;
        } else if (nProcessors < 9) {
            n=4;
        } else if (nProcessors < 17) {
            n=5;
        } else {
            n=6;
        }
        logger.info("Number of selector threads computed dynamically. Processors: " + nProcessors + ", selectors: "+ n);
        return n;
    }

    /**
     * Starts listening and handling client connections.
     * 
     * @throws IOException if error occurs while preparing socket channel
     */
    public void start() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(localPort);
        serverSocketChannel.socket().bind(address);

        SelectorThread selectorThread = getNextThread();
        selectorThread.scheduleRegisterChannel(serverSocketChannel, SelectionKey.OP_ACCEPT, this);
        for (int i = 0; i < selectorThreads.length; i++) {
            selectorThreads[i].start();
        }
        started = true;
    }
    

    public boolean isStarted() {
        return started ;
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
            // TODO: probably too many open files exception,
            // but i don't know what to do then; is server socket channel valid
            // after throwing this exception?; if yes can we just ignore it and
            // wait for new connections?
            throw new RuntimeException(e);
        }
        selectorThreads[0].addChannelInterest(serverSocketChannel, SelectionKey.OP_ACCEPT);

        // if accepting was successful create new client proxy
        if (socketChannel != null) {
            try {
                SelectorThread selectorThread = getNextThread();
                ReaderAndWriter raw = new ReaderAndWriter(socketChannel, selectorThread);
                new NioClientProxy(raw, requestManager, idGenerator);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Connection from " + socketChannel.socket().getInetAddress());
                }
            } catch (IOException e) {
                // TODO: probably registering to selector has failed; should we
                // just close the client connection?
                e.printStackTrace();
            }
        }
    }

    private SelectorThread getNextThread() {
        SelectorThread t = selectorThreads[nextThread];
        nextThread = (nextThread+1) % selectorThreads.length;
        return t;
    }

    private final static Logger logger = Logger.getLogger(NioClientManager.class.getCanonicalName());

}
