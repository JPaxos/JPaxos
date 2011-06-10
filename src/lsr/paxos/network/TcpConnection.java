package lsr.paxos.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.KillOnExceptionHandler;
import lsr.common.PID;
import lsr.common.ProcessDescriptor;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

/**
 * This class is responsible for handling stable TCP connection to other
 * replica, provides two methods for establishing new connection: active and
 * passive. In active mode we try to connect to other side creating new socket
 * and connects. If passive mode is enabled, then we wait for socket from the
 * <code>SocketServer</code> provided by <code>TcpNetwork</code>.
 * <p>
 * Every time new message is received from this connection, it is deserialized,
 * and then all registered network listeners in related <code>TcpNetwork</code>
 * are notified about it.
 * 
 * @see TcpNetwork
 */
public class TcpConnection {
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private final PID replica;
    private volatile boolean connected = false;
    /** true if connection should be started by this replica; */
    private final boolean active;
    private final TcpNetwork network;

    private final Thread senderThread;
    private final Thread receiverThread;

    private final ArrayBlockingQueue<byte[]> sendQueue = new ArrayBlockingQueue<byte[]>(128);

    /**
     * Creates a new TCP connection to specified replica.
     * 
     * @param network - related <code>TcpNetwork</code>.
     * @param replica - replica to connect to.
     * @param active - initiates connection if true; waits for remote connection
     *            otherwise.
     */
    public TcpConnection(TcpNetwork network, PID replica, boolean active) {
        this.network = network;
        this.replica = replica;
        this.active = active;

        logger.info("Creating connection: " + replica + " - " + active);

        this.receiverThread = new Thread(new ReceiverThread(), "TcpReceiver" + this.replica.getId());
        this.senderThread = new Thread(new Sender(), "TcpSender" + this.replica.getId());
        receiverThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
        senderThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
    }

    /**
     * Starts the receiver and sender thread.
     */
    public synchronized void start() {
        receiverThread.start();
        senderThread.start();
    }

    final class Sender implements Runnable {
        public void run() {
            logger.info("Sender thread started.");
            try {
                while (true) {
                    byte[] msg = sendQueue.take();
                    // ignore message if not connected
                    // Works without memory barrier because connected is volatile
                    if (!connected) {
                        continue;
                    }

                    try {
                        output.write(msg);
                        output.flush();
                    } catch (IOException e) {
                        logger.log(Level.WARNING, "Error sending message", e);
                    }
                }
            } catch (InterruptedException e) {
                logger.severe("Sender thread has been interupted and stopped.");
            }
        }
    }

    /**
     * Main loop used to connect and read from the socket.
     */
    final class ReceiverThread implements Runnable {
        public void run() {
            while (true) {
                // wait until connection is established
                logger.info("Waiting for tcp connection to " + replica.getId());

                try {
                    connect();
                } catch (InterruptedException e) {
                    logger.severe("Receiver thread has been interupted.");
                    break;
                }
                logger.info("Tcp connected " + replica.getId());

                while (true) {
                    if (Thread.interrupted()) {
                        logger.severe("Receiver thread has been interupted.");
                        close();
                        return;
                    }

                    Message message;
                    try {
                        message = MessageFactory.create(input);
                    } catch (IllegalArgumentException e) {
                        // end of stream or problem with socket occurred so
                        // close connection and try to establish it again
                        logger.log(Level.SEVERE, "Error deserializing msg", e);
                        close();
                        break;
                    }
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("Received [" + replica.getId() + "] " + message +
                                    " size: " + message.byteSize());
                    }
                    network.fireReceiveMessage(message, replica.getId());
                }
            }
        }
    }

    /**
     * Sends specified binary packet using underlying TCP connection.
     * 
     * @param message - binary packet to send
     * @return true if sending message was successful
     */
    public boolean send(byte[] message) {
        try {
            sendQueue.put(message);
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread interrupted", e);
        }
        return true;
    }

    /**
     * Registers new socket to this TCP connection. Specified socket should be
     * initialized connection with other replica. First method tries to close
     * old connection and then set-up new one.
     * 
     * @param socket - active socket connection
     * @param input - input stream from this socket
     * @param output - output stream from this socket
     */
    public synchronized void setConnection(Socket socket, DataInputStream input,
                                           DataOutputStream output) {
        assert socket.isConnected() : "Invalid socket state";

        // first close old connection
        close();

        // initialize new connection
        this.socket = socket;
        this.input = input;
        this.output = output;
        connected = true;

        // if main thread wait for this connection notify it
        notifyAll();
    }

    /**
     * Stops current connection and stops all underlying threads.
     * 
     * Note: This method waits until all threads are finished.
     * 
     * @throws InterruptedException
     */
    public void stop() throws InterruptedException {
        close();
        receiverThread.interrupt();
        senderThread.interrupt();

        receiverThread.join();
        senderThread.join();
    }

    /**
     * Establishes connection to host specified by this object. If this is
     * active connection then it will try to connect to other side. Otherwise we
     * will wait until connection will be set-up using
     * <code>setConnection</code> method. This method will return only if the
     * connection is established and initialized properly.
     * 
     * @throws InterruptedException
     */
    private void connect() throws InterruptedException {
        if (active) {
            // this is active connection so we try to connect to host
            while (true) {
                try {
                    socket = new Socket();
                    socket.setReceiveBufferSize(128 * 1024);
                    socket.setSendBufferSize(128 * 1024);
                    logger.fine("RcvdBuffer: " + socket.getReceiveBufferSize() + ", SendBuffer: " +
                                socket.getSendBufferSize());
                    socket.setTcpNoDelay(true);

                    logger.info("Connecting to: " + replica);
                    try {
                        socket.connect(new InetSocketAddress(replica.getHostname(),
                                replica.getReplicaPort()));
                    } catch (ConnectException e) {
                        logger.warning("TCP connection with replica " + replica.getId() + " failed");
                        Thread.sleep(ProcessDescriptor.getInstance().tcpReconnectTimeout);
                        continue;
                    }

                    input = new DataInputStream(
                            new BufferedInputStream(socket.getInputStream()));
                    output = new DataOutputStream(
                            new BufferedOutputStream(socket.getOutputStream()));

                    output.writeInt(ProcessDescriptor.getInstance().localId);
                    output.flush();
                    // connection established
                    break;
                } catch (IOException e) {
                    // some other problem (possibly other side closes
                    // connection while initializing connection); for debug
                    // purpose we print this message
                    logger.log(Level.WARNING, "Error connecting to " + replica, e);
                }
                Thread.sleep(ProcessDescriptor.getInstance().tcpReconnectTimeout);
            }

            // Wake up the sender thread
            synchronized (this) {
                connected = true;
                notifyAll();
            }

        } else {
            // this is passive connection so we are waiting until other replica
            // connect to us; we will be notified by setConnection method
            synchronized (this) {
                while (!connected) {
                    wait();
                }
            }
        }
    }

    /**
     * Closes the connection.
     */
    private synchronized void close() {
        if (socket != null && socket.isConnected()) {
            logger.info("Closing socket ...");
            try {
                socket.shutdownOutput();

                // TODO not clean socket closing; we have to wait until all data
                // will be received from server; after closing output stream we
                // should wait until we read all data from input stream;
                // otherwise RST will be send
                socket.close();
                socket = null;
                logger.info("Socket closed.");
            } catch (IOException e) {
                logger.warning("Error closing socket: " + e.getMessage());
            }
        }
        connected = false;
    }

    private final static Logger logger = Logger.getLogger(TcpConnection.class.getCanonicalName());
}
