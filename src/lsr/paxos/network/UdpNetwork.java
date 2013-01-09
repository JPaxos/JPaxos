package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Configuration;
import lsr.common.KillOnExceptionHandler;
import lsr.common.PID;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

/**
 * Represents network based on UDP. Provides basic methods, sending messages to
 * other replicas and receiving messages. This class didn't provide any
 * guarantee that sent message will be received by target. It is possible that
 * some messages will be lost.
 * 
 */
public class UdpNetwork extends Network {
    private final DatagramSocket datagramSocket;
    private final Thread readThread;
    private final SocketAddress[] addresses;
    private boolean started = false;

    /**
     * @throws SocketException
     */
    public UdpNetwork() throws SocketException {
        addresses = new SocketAddress[processDescriptor.numReplicas];
        for (int i = 0; i < addresses.length; i++) {
            PID pid = processDescriptor.config.getProcess(i);
            addresses[i] = new InetSocketAddress(pid.getHostname(), pid.getReplicaPort());
        }

        int localPort = processDescriptor.getLocalProcess().getReplicaPort();
        logger.info("Opening port: " + localPort);
        datagramSocket = new DatagramSocket(localPort);

        datagramSocket.setReceiveBufferSize(Configuration.UDP_RECEIVE_BUFFER_SIZE);
        datagramSocket.setSendBufferSize(Configuration.UDP_SEND_BUFFER_SIZE);

        readThread = new Thread(new SocketReader(), "UdpReader");
        readThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
        readThread.setDaemon(true);
    }

    @Override
    public void start() {
        if (!started) {
            readThread.start();
            started = true;
        } else {
            logger.warning("Starting UDP networkmultiple times!");
        }
    }

    /**
     * Reads messages from the network and enqueues them to be handled by the
     * dispatcher thread.
     */
    private class SocketReader implements Runnable {
        public void run() {
            logger.info(Thread.currentThread().getName() +
                        " thread started. Waiting for UDP messages");
            try {
                while (!Thread.interrupted()) {
                    byte[] buffer = new byte[processDescriptor.maxUdpPacketSize + 4];
                    // Read message and enqueue it for processing.
                    DatagramPacket dp = new DatagramPacket(buffer, buffer.length);
                    datagramSocket.receive(dp);

                    ByteArrayInputStream bais = new ByteArrayInputStream(dp.getData(),
                            dp.getOffset(), dp.getLength());
                    DataInputStream dis = new DataInputStream(bais);

                    int sender = dis.readInt();
                    byte[] data = new byte[dp.getLength() - 4];
                    dis.read(data);

                    try {
                        Message message = MessageFactory.readByteArray(data);
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Received from " + sender + ":" + message);
                        }
                        fireReceiveMessage(message, sender);
                    } catch (ClassNotFoundException e) {
                        logger.log(Level.WARNING, "Error deserializing message", e);
                    }
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Fatal error.", e);
                throw new RuntimeException(e);
            }
            logger.log(Level.SEVERE, "UDP network interrupted.");
        }
    }

    /**
     * Blocks until there is space in the OS to buffer the message. Normally it
     * should return immediately. Specified byte array should be serialized
     * message (without any header like id of replica).
     * <p>
     * The sentMessage event in listeners is not fired after calling this
     * method.
     * 
     * @param message - the message to send
     * @param destinations - the id's of replicas to send message to
     * @throws IOException if an I/O error occurs
     */
    protected void send(byte[] message, BitSet destinations) {
        // prepare packet to send
        byte[] data = new byte[message.length + 4];
        ByteBuffer.wrap(data).putInt(processDescriptor.localId).put(message);
        DatagramPacket dp = new DatagramPacket(data, data.length);

        for (int i = destinations.nextSetBit(0); i >= 0; i = destinations.nextSetBit(i + 1)) {
            dp.setSocketAddress(addresses[i]);
            try {
                datagramSocket.send(dp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void send(Message message, BitSet destinations) {
        message.setSentTime();
        byte[] messageBytes = message.toByteArray();
        if (messageBytes.length > processDescriptor.maxUdpPacketSize + 4) {
            throw new RuntimeException("Data packet too big. Size: " +
                                       messageBytes.length + ", limit: " +
                                       processDescriptor.maxUdpPacketSize +
                                       ". Packet not sent.");
        }

        send(messageBytes, destinations);
    }

    private final static Logger logger = Logger.getLogger(UdpNetwork.class.getCanonicalName());

    @Override
    protected void send(Message message, int destination) {
        // prepare packet to send
        byte[] data = new byte[message.byteSize() + 4];
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.putInt(processDescriptor.localId);
        message.writeTo(bb);

        DatagramPacket dp = new DatagramPacket(data, data.length);

        dp.setSocketAddress(addresses[destination]);

        try {
            datagramSocket.send(dp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}