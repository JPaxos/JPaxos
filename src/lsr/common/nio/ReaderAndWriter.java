package lsr.common.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides default implementation of <code>ReadWriteHandler</code>
 * using java channels. It provides method used to send byte array, which will
 * be send as soon as there will be space in system send buffer. Reading data is
 * done using <code>PacketHandler</code>. After setting new
 * <code>PacketHandler</code> to this object, reading mode is enabled, and reads
 * data to fill entire byte buffer(provided by <code>PacketHandler</code>). If
 * no space remains available in read buffer, <code>PacketHandler</code> is
 * notified by calling <code>finish</code> method on it. The handler is removed
 * after reading whole packet, so it has to be set again.
 * 
 * @see PacketHandler
 */
public class ReaderAndWriter implements ReadWriteHandler {
    private final SelectorThread selectorThread;
    public final SocketChannel socketChannel;
    private final Queue<byte[]> messages;
    private PacketHandler packetHandler;
    private ByteBuffer writeBuffer;

    /**
     * Creates new <code>ReaderAndWrite</code> using socket channel and selector
     * thread. It will also register this socket channel into selector.
     * 
     * @param socketChannel - channel used to read and write data
     * @param selectorThread - selector which will handle all operations from
     *            this channel
     * @throws IOException if registering channel to selector has failed
     */
    public ReaderAndWriter(SocketChannel socketChannel, SelectorThread selectorThread)
            throws IOException {
        this.socketChannel = socketChannel;
        // NS: Disable Nagle's algorithm to improve performance with small
        // answers.
        this.socketChannel.socket().setTcpNoDelay(true);
        this.selectorThread = selectorThread;
        this.messages = new LinkedList<byte[]>();
        this.selectorThread.registerChannel(socketChannel, 0, this);
    }

    /**
     * Registers new packet handler. All received data will be written into its
     * buffer. The reading will be activated on underlying channel.
     * 
     * @param packetHandler the packet handler to set
     */
    public void setPacketHandler(PacketHandler packetHandler) {
        assert this.packetHandler == null : "Previous packet wasn't read yet.";
        this.packetHandler = packetHandler;
        selectorThread.scheduleAddChannelInterest(socketChannel, SelectionKey.OP_READ);
    }

    /**
     * This method is called from selector thread to notify that there are new
     * data available in socket channel.
     */
    public void handleRead() {
        try {
            while (packetHandler != null) {
                int readBytes = socketChannel.read(packetHandler.getByteBuffer());

                // no more data in system buffer
                if (readBytes == 0)
                    break;

                // EOF - that means that the other side close his socket, so we
                // should close this connection too.
                if (readBytes == -1) {
                    innerClose();
                    return;
                }

                // if the whole packet was read, then notify packet handler;
                // calling return instead of break cause that the OP_READ flag
                // is not set ; to start reading again, new packet handler has
                // to be set
                if (packetHandler.getByteBuffer().remaining() == 0) {
                    PacketHandler old = packetHandler;
                    packetHandler = null;
                    old.finished();
                    return;
                }
                break;
            }
        } catch (IOException e) {
            innerClose();
            return;
        }
        selectorThread.addChannelInterest(socketChannel, SelectionKey.OP_READ);
    }

    /**
     * This method is called from selector thread to notify that there is free
     * space in system send buffer, and it is possible to send new packet of
     * data.
     */
    public void handleWrite() {
        synchronized (messages) {
            // try to send all messages
            while (!messages.isEmpty()) {
                // create buffer from first message
                if (writeBuffer == null)
                    writeBuffer = ByteBuffer.wrap(messages.peek());

                // write as many bytes as possible
                int writeBytes = 0;
                try {
                    writeBytes = socketChannel.write(writeBuffer);
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Error writing to socket", e);
                    innerClose();
                    return;
                }

                // cannot write more so break
                if (writeBytes == 0)
                    break;

                // remove message after sending
                if (writeBuffer.remaining() == 0) {
                    writeBuffer = null;
                    messages.poll();
                }
            }
            // if there are messages to send, add interest in writing
            if (!messages.isEmpty())
                selectorThread.addChannelInterest(socketChannel, SelectionKey.OP_WRITE);
        }
    }

    /**
     * Adds the message to the queue of messages to sent. This method is
     * asynchronous and will return immediately.
     * 
     * @param message
     */
    public void send(byte[] message) {
        // discard message if channel is not connected
        if (!socketChannel.isConnected())
            return;

        synchronized (messages) {
            messages.add(message);

            // if writing is not active, activate it
            if (writeBuffer == null)
                selectorThread.scheduleAddChannelInterest(socketChannel, SelectionKey.OP_WRITE);
        }
    }

    /**
     * Closes the underlying socket channel.
     */
    public void close() {
        selectorThread.beginInvoke(new Runnable() {
            public void run() {
                innerClose();
            }
        });
    }

    /**
     * Closes the underlying socket channel. It closes channel immediately so it
     * should be called only from selector thread.
     */
    private void innerClose() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            logger.warning("Error closing socket: " + e.getMessage());
        }
    }

    private final static Logger logger = Logger.getLogger(ReaderAndWriter.class.getCanonicalName());
}