package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.KillOnExceptionHandler;
import lsr.paxos.messages.Message;

public class TcpNetwork extends Network implements Runnable {
    private final TcpConnection[] connections;
    private final ServerSocket server;
    private final Thread acceptorThread;
    private boolean started = false;

    /**
     * Creates new network for handling connections with other replicas.
     * 
     * @throws IOException if opening server socket fails
     */
    public TcpNetwork() throws IOException {
        super();
        this.connections = new TcpConnection[processDescriptor.numReplicas];
        logger.fine("Opening port: " + processDescriptor.getLocalProcess().getReplicaPort());
        this.server = new ServerSocket();
        server.setReceiveBufferSize(256 * 1024);
        server.bind(new InetSocketAddress((InetAddress) null,
                processDescriptor.getLocalProcess().getReplicaPort()));

        this.acceptorThread = new Thread(this, "TcpNetwork");
        acceptorThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
    }

    @Override
    public synchronized void start() {
        if (!started) {
            logger.fine("Starting TcpNetwork");
            for (int i = 0; i < connections.length; i++) {
                if (i < processDescriptor.localId) {
                    connections[i] = new TcpConnection(this,
                            processDescriptor.config.getProcess(i), false);
                    connections[i].start();
                }
                if (i > processDescriptor.localId) {
                    connections[i] = new TcpConnection(this,
                            processDescriptor.config.getProcess(i), true);
                    connections[i].start();
                }
            }
            // Start the thread that listens and accepts new connections.
            // Must be started after the connections are initialized (code
            // above)
            acceptorThread.start();
            started = true;
        } else {
            logger.warning("Starting TcpNetwork multiple times!");
        }
    }

    /**
     * Sends binary data to specified destination.
     * 
     * @param message - binary data to send
     * @param destination - id of replica to send data to
     * @return true if message was sent; false if some error occurred
     */
    @Deprecated
    public boolean send(byte[] message, int destination) {
        return sendBytes(message, destination);
    }

    /* package access */boolean sendBytes(byte[] message, int destination) {
        assert destination != processDescriptor.localId;
        return connections[destination].send(message);
    }

    /**
     * Main loop which accepts incoming connections.
     */
    public void run() {
        logger.info(Thread.currentThread().getName() + " thread started");
        while (true) {
            try {
                Socket socket = server.accept();
                initializeConnection(socket);
            } catch (IOException e) {
                // TODO: probably too many open files exception occurred;
                // should we open server socket again or just wait and ignore
                // this exception?
                throw new RuntimeException(e);
            }
        }
    }

    private void initializeConnection(Socket socket) {
        try {
            logger.info("Received connection from " + socket.getRemoteSocketAddress());
            socket.setSendBufferSize(128 * 1024);
            socket.setTcpNoDelay(true);
            DataInputStream input = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            OutputStream output = socket.getOutputStream();
            int replicaId = input.readInt();

            if (replicaId < 0 || replicaId >= processDescriptor.numReplicas) {
                logger.warning("Remoce host id is out of range: " + replicaId);
                socket.close();
                return;
            }

            assert replicaId != processDescriptor.localId : "Remote replica has same id as local";

            connections[replicaId].setConnection(socket, input, output);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Initialization of accepted connection failed.", e);
            try {
                socket.close();
            } catch (IOException e1) {
            }
        }
    }

    public void sendMessage(Message message, BitSet destinations) {
        assert !destinations.isEmpty() : "Sending a message to no one";

        // do not send message to self (just fire event)
        if (destinations.get(processDescriptor.localId)) {
            logger.warning("Sending message to self: " + message);
            fireReceiveMessage(message, processDescriptor.localId);
            destinations = (BitSet) destinations.clone();
            destinations.clear(processDescriptor.localId);
        }

        byte[] bytes = message.toByteArray();
        for (int i = destinations.nextSetBit(0); i >= 0; i = destinations.nextSetBit(i + 1)) {
            sendBytes(bytes, i);
        }

        // Not really sent, only queued for sending,
        // but it's good enough for the notification
        fireSentMessage(message, destinations);
    }

    public void sendMessage(Message message, int destination) {
        BitSet target = new BitSet();
        target.set(destination);
        sendMessage(message, target);
    }

    public void sendToAllButMe(Message message) {
        sendMessage(message, allButMe);
    }

    public void closeAll() {
        for (TcpConnection c : connections) {
            try {
                if (c != null)
                    c.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private final static Logger logger = Logger.getLogger(TcpNetwork.class.getCanonicalName());
}
