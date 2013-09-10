package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import lsr.common.ProcessDescriptor;
import lsr.common.nio.AcceptHandler;
import lsr.common.nio.ReaderAndWriter;
import lsr.common.nio.SelectorThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private int nextThread = 0;
    private final ClientRequestManager requestManager;
    private ServerSocketChannel serverSocketChannel = null;

    /**
     * Creates new client manager.
     * 
     * @param localPort - the listen port for client connections
     * @param requestManager - callback invoked every time new message is
     *            received by client
     * @param idGenerator - generator used to allocate id's for clients
     * @throws IOException if creating selector failed
     */
    public NioClientManager(ClientRequestManager requestManager)
            throws IOException {
        this.requestManager = requestManager;
        requestManager.setClientManager(this);

        int nSelectors = processDescriptor.selectorThreadCount;
        if (nSelectors == -1) {
            nSelectors = NioClientManager.computeNSelectors();
        } else {
            if (nSelectors < 0) {
                throw new IOException("Invalid value for property " +
                                      ProcessDescriptor.SELECTOR_THREADS + ": " + nSelectors);
            }
        }
        logger.info("Real {}={}", ProcessDescriptor.SELECTOR_THREADS, nSelectors);

        selectorThreads = new SelectorThread[nSelectors];
        for (int i = 0; i < selectorThreads.length; i++) {
            selectorThreads[i] = new SelectorThread(i);
        }
    }

    public void executeInAllSelectors(Runnable r) {
        for (SelectorThread sThread : selectorThreads) {
            sThread.beginInvoke(r);
        }
    }

    /**
     * Starts listening and handling client connections.
     * 
     * @throws IOException if error occurs while preparing socket channel
     */
    public void start() throws IOException {
        assert serverSocketChannel == null : "Start called more than once";
        serverSocketChannel = ServerSocketChannel.open();
        int localPort = processDescriptor.getLocalProcess().getClientPort();
        serverSocketChannel.socket().bind(new InetSocketAddress(localPort));

        SelectorThread selectorThread = getNextThread();
        selectorThread.scheduleRegisterChannel(serverSocketChannel, SelectionKey.OP_ACCEPT, this);
        for (int i = 0; i < selectorThreads.length; i++) {
            selectorThreads[i].start();
        }
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
            /*
             * TODO: (NS) probably too many open files exception, but i don't
             * know what to do then; is server socket channel valid after
             * throwing this exception?; if yes can we just ignore it and wait
             * for new connections?
             */
            throw new RuntimeException(e);
        }
        selectorThreads[0].addChannelInterest(serverSocketChannel, SelectionKey.OP_ACCEPT);

        assert socketChannel != null;

        try {
            SelectorThread selectorThread = getNextThread();
            ReaderAndWriter raw = new ReaderAndWriter(socketChannel, selectorThread);
            new NioClientProxy(raw, requestManager);
            if (logger.isDebugEnabled()) {
                logger.debug("Connection from {}", socketChannel.socket().getInetAddress());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to set tcpNoDelay somewhere below. Let's die.",
                    e);
        }
    }

    public SelectorThread getNextThread() {
        SelectorThread t = selectorThreads[nextThread];
        nextThread = (nextThread + 1) % selectorThreads.length;
        return t;
    }

    private static int computeNSelectors() {
        int nProcessors = Runtime.getRuntime().availableProcessors();
        int n;

        /*
         * (NS) Values determined empirically based on tests on 24 core Opteron
         * system.
         */
        if (nProcessors < 3) {
            n = 1;
        } else if (nProcessors < 5) {
            n = 2;
        } else if (nProcessors < 7) {
            n = 3;
        } else if (nProcessors < 9) {
            n = 4;
        } else if (nProcessors < 17) {
            n = 5;
        } else {
            n = 6;
        }
        logger.info(
                "Number of selector threads selected basing on static tables. Processors: {}, selectors: ",
                nProcessors, n);
        return n;
    }

    private final static Logger logger = LoggerFactory.getLogger(NioClientManager.class);

}
