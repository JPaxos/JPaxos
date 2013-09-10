package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import lsr.common.PID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NioOutputConnection extends NioConnection {

    private static final int MAX_PENDING_DATA = 1024;
    private static final int DRAIN_LIMIT = 64;
    private static final int BUFFER_SIZE = 8192;

    private BlockingQueue<byte[]> pendingData = new ArrayBlockingQueue<byte[]>(
            MAX_PENDING_DATA);
    private ArrayDeque<byte[]> drained = new ArrayDeque<byte[]>(DRAIN_LIMIT);

    ByteBuffer outputBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    byte[] currentData;
    int currentDataWritten = 0;

    private volatile boolean connected = false;

    public NioOutputConnection(NioNetwork network, PID replica, SocketChannel channel)
            throws IOException {
        super(network, replica, channel);
        if (channel != null) {
            channel.register(selector, SelectionKey.OP_WRITE);
            connected = true;
            outputBuffer.flip();
            logger.debug("output connection established to: {}", replicaId);
        }

        // if (network.localId == 0)
        // pendingData = new ArrayBlockingQueue<byte[]>(100 * 1024);
    }

    public boolean send(byte[] data) {
        // if (!connected)
        // return false;

        try {
            boolean success = pendingData.offer(data);
            if (!success) {
                // long time = System.currentTimeMillis();
                pendingData.put(data);
                // Network.writeWaitTime.add(data[4], System.currentTimeMillis()
                // - time);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    protected void tryConnect() throws IOException {
        logger.debug("trying to connect to: {}", replicaId);

        channel = SocketChannel.open();
        network.configureChannel(channel);
        channel.connect(new InetSocketAddress(replica.getHostname(),
                replica.getReplicaPort()));

        channel.register(selector, SelectionKey.OP_CONNECT);
    }

    protected void introduce() {
        logger.trace("introducing myself to: {}", replicaId);

        // ByteBuffer myIdByteBuffer = ByteBuffer.allocate(4);
        // myIdByteBuffer.putInt(network.localId);
        // myIdByteBuffer.flip();

        outputBuffer.clear();
        outputBuffer.putInt(4);
        outputBuffer.putInt(Network.localId);
        outputBuffer.flip();

        // selector.wakeup();
        // pendingData.offer(myIdByteBuffer);
    }

    @Override
    public void run() {
        String name = "NioOutputConnection_" + Network.localId + "->" + replicaId;
        setName(name);
        logger.debug("starting {}", name);

        while (true) {
            try {
                if (!connected) {
                    disposeConnection();
                    if (isActiveConnection())
                        tryConnect();
                    else
                        return;
                } else {
                    if (outputBuffer.remaining() == 0)
                        fillBuffer();
                }

                selector.select();

                Iterator<SelectionKey> selectedKeys = selector.selectedKeys()
                    .iterator();
                while (selectedKeys.hasNext()) {
                    SelectionKey key = (SelectionKey) selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid())
                        continue;

                    if (key.isConnectable())
                        finishConnect(key);
                    if (key.isWritable())
                        write(key);
                    // if (key.isReadable())
                    // disposeConnection();
                }
            } catch (Exception e) {
                logger.debug("output connection problem with: {}", replicaId);
                connected = false;
            }
            if (!connected && isActiveConnection()) {
                try {
                    sleep(processDescriptor.tcpReconnectTimeout);
                } catch (InterruptedException e) {
                    logger.trace("waking up early to try to connect");
                }
            }
        }
    }

    private void fillBuffer() throws InterruptedException {
        logger.trace("filling buffer");
        outputBuffer.clear();
        while (true) {
            if (currentData != null) {
                logger.trace("copying data to buffer");
                int bytesToCopy = Math.min(currentData.length
                                           - currentDataWritten, outputBuffer.remaining());
                outputBuffer.put(currentData, currentDataWritten, bytesToCopy);
                currentDataWritten += bytesToCopy;
                if (currentDataWritten == currentData.length) {
                    logger.trace("currentData written entirely");
                    currentData = null;
                }
            }
            if (outputBuffer.remaining() < 4
                || (outputBuffer.position() != 0 && noPendingData())) {
                logger.trace("preparing buffer for sending");
                outputBuffer.flip();
                return;
            }
            if (currentData == null) {
                logger.trace("taking new data from the queue");
                currentData = getNewData();
                currentDataWritten = 0;
                outputBuffer.putInt(currentData.length);
            }
        }
    }

    private boolean noPendingData() {
        // return pendingData.isEmpty();
        if (!drained.isEmpty())
            return false;
        pendingData.drainTo(drained, DRAIN_LIMIT);
        return drained.isEmpty();
    }

    private byte[] getNewData() throws InterruptedException {
        // return pendingData.take();

        if (!drained.isEmpty())
            return drained.removeFirst();
        else {
            pendingData.drainTo(drained, DRAIN_LIMIT);
            if (!drained.isEmpty())
                return drained.removeFirst();
            else {
                return pendingData.take();
            }
        }
    }

    private void write(SelectionKey key) throws IOException {
        int count = channel.write(outputBuffer);

        logger.trace("writing to: {} ( bytes)", replicaId, count);
    }

    private void finishConnect(SelectionKey key) throws IOException {
        channel.finishConnect();
        connected = true;
        key.interestOps(SelectionKey.OP_WRITE);

        logger.debug("output connection established to: {} (finishConnect)", replicaId);

        NioInputConnection inputConnection = new NioInputConnection(network,
                replica, channel);
        network.connections[replicaId][0] = inputConnection;
        inputConnection.start();

        introduce();
    }

    public void notifyAboutInputConnected() {
        if (!connected) {
            logger.debug(
                    "got notification about input established, trying to establish output: {}",
                    replicaId);
            this.interrupt();
        }
    }

    public void notifyAboutInputDisconnected() {
        connected = false;
        this.interrupt();
        logger.debug("got notification about input disconnected, disconnecting myself: {}",
                replicaId);
    }

    protected void disposeConnection() {
        connected = false;
        currentData = null;
        for (SelectionKey key : selector.keys())
            key.cancel();
        pendingData.clear();

        logger.debug("output connection closed with: {}", replicaId);

        network.removeConnection(Network.localId, replicaId);
    }

    private final static Logger logger = LoggerFactory.getLogger(Network.class);
}
