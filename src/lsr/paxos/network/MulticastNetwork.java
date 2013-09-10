package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;

import lsr.common.KillOnExceptionHandler;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MulticastNetwork extends Network {

    private static final int myHeaderSize = 1 + 8 + 4 + 4;
    private final MulticastSocket socket;
    private final Network unicastNetwork;
    private final InetSocketAddress groupAddress;
    private final Thread readThread;

    public MulticastNetwork(Network unicastNetwork, long runId) throws IOException {
        super();
        this.sendMessageId = runId << 32 + localId;

        if (processDescriptor.numReplicas > 256) {
            throw new RuntimeException("Multicast network supports up to 256 relicas");
        }

        this.unicastNetwork = unicastNetwork;
        socket = new MulticastSocket(processDescriptor.multicastPort);
        // MulticastSocket constructor by default sets SO_REUSEADDR

        groupAddress = new InetSocketAddress(
                InetAddress.getByName(processDescriptor.multicastIpAddress),
                processDescriptor.multicastPort);
        socket.joinGroup(groupAddress.getAddress());

        readThread = new ReceiveThread();

        logger.info("Multicast network created on {} to {} and joined {}",
                socket.getLocalAddress(), socket.getInetAddress(), groupAddress);
    }

    protected void send(Message message, int destination) {
        unicastNetwork.send(message, destination);
    }

    protected void send(Message message, BitSet destinations) {
        if (shouldMulticast(destinations)) {
            try {
                multicast(message);
            } catch (IOException e) {
                throw new RuntimeException("Connectionless socket refused a send operation", e);
            }
        } else {
            logger.trace("Multicast network - passing message to unicast netwrok: {}", message);
            unicastNetwork.send(message, destinations);
        }
    }

    private final Object sendLock = new Object();
    private ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final int payloadSize = processDescriptor.mtu - 8 /* udp header size */- myHeaderSize;
    private long sendMessageId;

    private final HashMap<Long, Message> recentMessages = new HashMap<Long, Message>();

    private final byte[] messageBA = new byte[processDescriptor.mtu - 8];
    private final ByteBuffer messageBuffer = ByteBuffer.wrap(messageBA);

    private void multicast(Message message) throws IOException {
        synchronized (sendLock) {
            if (sendBuffer.capacity() < message.byteSize())
                sendBuffer = ByteBuffer.allocate(message.byteSize());
            assert sendBuffer.position() == 0 && sendBuffer.limit() == sendBuffer.capacity();
            message.writeTo(sendBuffer);
            sendBuffer.flip();

            int size = sendBuffer.limit();
            int part = 0;

            messageBuffer.rewind();
            messageBuffer.put((byte) localId);
            messageBuffer.putLong(sendMessageId);
            messageBuffer.putInt(size);

            recentMessages.put(sendMessageId, message);

            logger.trace("Multicasting as {}: {}", sendMessageId, message);

            sendMessageId += processDescriptor.numReplicas;

            while (sendBuffer.remaining() > payloadSize) {
                sendBuffer.get(messageBA, myHeaderSize, payloadSize);
                multicast(part++, payloadSize + myHeaderSize);
            }

            int remaining = sendBuffer.remaining();
            if (remaining > 0) {
                sendBuffer.get(messageBA, myHeaderSize, remaining);
                multicast(part++, myHeaderSize + remaining);
            }

            sendBuffer.clear();
        }
    }

    private void multicast(int part, int size) throws IOException {
        logger.trace("Sending part {} size {}", part, size);
        messageBuffer.putInt(1 + 8 + 4, part);
        DatagramPacket dp = new DatagramPacket(messageBA, size);
        dp.setSocketAddress(groupAddress);
        socket.send(dp);
    }

    private boolean shouldMulticast(BitSet destinations) {
        return destinations.cardinality() > 1;
    }

    public void start() {
        assert !readThread.isAlive();
        readThread.start();
        unicastNetwork.start();
    }

    public class ReceiveThread extends Thread {

        private class MessageParts {
            private final BitSet bs = new BitSet();
            private final byte[] contents;
            private final int parts;

            public MessageParts(int length) {
                contents = new byte[length];
                parts = (length + payloadSize - 1) / payloadSize;
            }

            public void addPart(int part) {
                assert part < parts;
                if (bs.get(part)) {
                    logger.debug("Received part {} multiple times for some message", part);
                }
                bs.set(part);
                receiveBuffer.get(contents, part * payloadSize, receiveBuffer.remaining());
            }

            public boolean hasAllParts() {
                return bs.cardinality() == parts;
            }

            public byte[] get() {
                assert hasAllParts();
                return contents;
            }
        }

        private final byte[] receiveBA = new byte[processDescriptor.mtu - 8];
        private final ByteBuffer receiveBuffer = ByteBuffer.wrap(receiveBA);
        private final DatagramPacket packet = new DatagramPacket(receiveBA,
                receiveBA.length);

        private final HashMap<Long, MessageParts> messageParts = new HashMap<Long, MulticastNetwork.ReceiveThread.MessageParts>();

        public ReceiveThread() {
            super("MulticastReceive");
            setUncaughtExceptionHandler(new KillOnExceptionHandler());
            setDaemon(true);
        }

        public void run() {
            try {
                while (true) {
                    socket.receive(packet);
                    int senderId = receiveBuffer.get();
                    if (senderId == localId) {
                        receiveBuffer.clear();
                        continue;
                    }
                    long messageId = receiveBuffer.getLong();
                    int totalSize = receiveBuffer.getInt();
                    int partNo = receiveBuffer.getInt();

                    receiveBuffer.limit(packet.getLength());

                    assert (partNo + 1) * payloadSize > totalSize
                            ? partNo * payloadSize + receiveBuffer.remaining() == totalSize
                            : receiveBuffer.remaining() == payloadSize;

                    MessageParts parts = messageParts.get(messageId);
                    if (parts == null) {
                        parts = new MessageParts(totalSize);
                        messageParts.put(messageId, parts);
                    }

                    if (logger.isTraceEnabled())
                        logger.trace("Multicast received from {} part {} of {}, size {}", senderId,
                                partNo, messageId, receiveBuffer.remaining());

                    parts.addPart(partNo);

                    if (parts.hasAllParts()) {
                        received(messageId, senderId, parts);
                    }

                    receiveBuffer.clear();
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Connectionless socket refused a receive operation  or  read from memory failed",
                        e);
            }
        }

        private void received(long messageId, int sender, MessageParts parts) throws IOException {
            Message msg = MessageFactory.readByteArray(parts.get());
            if (logger.isTraceEnabled())
                logger.trace("Multicast received {} from {}: {}", messageId, sender, msg);
            fireReceiveMessage(msg, sender);
            recentMessages.remove(messageId);
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(MulticastNetwork.class);

}
