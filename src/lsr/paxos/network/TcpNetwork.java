package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.KillOnExceptionHandler;
import lsr.paxos.messages.Message;

public class TcpNetwork extends Network implements Runnable {
    private final TcpConnection[][] activeConnections;
    private final List<TcpConnection> allConnections = new ArrayList<TcpConnection>();
    private final ServerSocket server;
    private final Thread acceptorThread;
    private boolean started = false;

    /**
     * Creates new network for handling connections with other replicas.
     * 
     * @throws IOException if opening server socket fails
     */
    public TcpNetwork() throws IOException {
        activeConnections = new TcpConnection[processDescriptor.numReplicas][2];

        logger.info("Opening port: " + processDescriptor.getLocalProcess().getReplicaPort());

        server = new ServerSocket();
        server.setReceiveBufferSize(256 * 1024);
        server.bind(new InetSocketAddress((InetAddress) null,
                processDescriptor.getLocalProcess().getReplicaPort()));

        acceptorThread = new Thread(this, "TcpNetwork");
        acceptorThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
    }

    @Override
    public void start() {
        if (!started) {
            for (int i = 0; i < activeConnections.length; i++) {
                activeConnections[i][0] = null;
                activeConnections[i][1] = null;
                if (i == processDescriptor.localId)
                    continue;
                TcpConnection tcpConn = new TcpConnection(this,
                        processDescriptor.config.getProcess(i), i, true);
                allConnections.add(tcpConn);
            }
            acceptorThread.start();
            started = true;
        } else {
            logger.warning("Starting TCP networkmultiple times!");
            assert false;
        }
    }

    /**
     * Sends binary data to specified destination.
     * 
     * @param message - binary data to send
     * @param destination - id of replica to send data to
     * @return true if message was sent; false if some error occurred
     */
    protected void send(byte[] message, int destination) {
        if (activeConnections[destination][0] != null)
            activeConnections[destination][0].send(message);
    }

    protected void send(Message message, int destination) {
        send(message.toByteArray(), destination);
    }

    @Override
    public void send(Message message, BitSet destinations) {
        assert !destinations.isEmpty() : "Sending a message to no one";

        byte[] bytes = message.toByteArray();
        for (int i = destinations.nextSetBit(0); i >= 0; i = destinations.nextSetBit(i + 1)) {
            send(bytes, i);
        }
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
                throw new RuntimeException(e);
            }
        }
    }

    private void initializeConnection(Socket socket) {
        try {
            logger.info("Received connection from " + socket.getRemoteSocketAddress());
            socket.setReceiveBufferSize(TcpConnection.TCP_BUFFER_SIZE);
            socket.setSendBufferSize(TcpConnection.TCP_BUFFER_SIZE);
            socket.setTcpNoDelay(true);
            logger.warning("Passive. RcvdBuffer: " + socket.getReceiveBufferSize() +
                           ", SendBuffer: " + socket.getSendBufferSize());
            DataInputStream input = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));
            int replicaId = input.readInt();

            if (replicaId < 0 || replicaId >= processDescriptor.numReplicas) {
                logger.warning("Remoce host id is out of range: " + replicaId);
                socket.close();
                return;
            }
            if (replicaId == processDescriptor.localId) {
                logger.warning("Remote replica has same id as local: " + replicaId);
                socket.close();
                return;
            }

            TcpConnection tcpConn = new TcpConnection(this,
                    processDescriptor.config.getProcess(replicaId), replicaId, false);
            tcpConn.setConnection(socket, input, output);

            addConnection(replicaId, tcpConn);

        } catch (IOException e) {
            logger.log(Level.WARNING, "Initialization of accepted connection failed.", e);
            try {
                socket.close();
            } catch (IOException e1) {
            }
        }
    }

    /* package private */void addConnection(int replicaId, TcpConnection tcpConn) {
        synchronized (activeConnections) {
            if (activeConnections[replicaId][0] == null) {
                activeConnections[replicaId][0] = tcpConn;
            } else {
                if (activeConnections[replicaId][1] == null) {
                    if (activeConnections[replicaId][0].isActive() ^ tcpConn.isActive()) {
                        activeConnections[replicaId][1] = tcpConn;
                    } else {
                        activeConnections[replicaId][0].stopAsync();
                        activeConnections[replicaId][0] = tcpConn;
                    }
                } else {
                    if (activeConnections[replicaId][1].isActive() ^ tcpConn.isActive()) {
                        activeConnections[replicaId][0].stopAsync();
                        activeConnections[replicaId][0] = tcpConn;
                    } else {
                        activeConnections[replicaId][1].stopAsync();
                        activeConnections[replicaId][1] = tcpConn;
                    }
                }
            }
        }
    }

    /* package private */void removeConnection(int replicaId, TcpConnection tcpConn) {
        synchronized (activeConnections) {
            if (activeConnections[replicaId][1] == tcpConn) {
                activeConnections[replicaId][1] = null;
            } else if (activeConnections[replicaId][0] == tcpConn) {
                activeConnections[replicaId][0] = activeConnections[replicaId][1];
                activeConnections[replicaId][1] = null;
            }

            if (!tcpConn.isActive())
                allConnections.remove(tcpConn);
        }
    }

    public void closeAll() {
        for (TcpConnection c : allConnections) {
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
