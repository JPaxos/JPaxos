package lsr.common.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReaderAndWriter implements ReadWriteHandler {
    private final SelectorThread selectorThread;
    public final SocketChannel socketChannel;

    private volatile ByteBuffer message;
    private PacketHandler packetHandler;

    /**
     * Creates new <code>ReaderAndWrite</code> using socket channel and selector
     * thread.
     * 
     * @param socketChannel - channel used to read and write data
     * @param selectorThread - selector which will handle all operations from
     *            this channel
     * @throws IOException if registering channel to selector has failed
     */
    public ReaderAndWriter(SocketChannel socketChannel, SelectorThread selectorThread)
            throws IOException {
        this.socketChannel = socketChannel;
        this.selectorThread = selectorThread;

        this.socketChannel.socket().setTcpNoDelay(true);
    }

    /**
     * Registers new packet handler. All received data will be written into its
     * buffer. It will also register this socket channel into selector.
     * 
     * @param packetHandler the packet handler to set
     */
    public void setPacketHandler(PacketHandler packetHandler) {
        this.packetHandler = packetHandler;
        if (!socketChannel.isRegistered()) {
            try {
                this.selectorThread.scheduleRegisterChannel(socketChannel, SelectionKey.OP_READ,
                        this);
            } catch (IOException e) {
                forceClose();
                logger.debug("Client connection faulted on config: {} ({})",
                        socketChannel.socket().getInetAddress(), e.getCause());
            }
        }
    }

    /**
     * This method is called from selector thread to notify that there are new
     * data available in socket channel.
     * 
     * @throws InterruptedException
     */
    @Override
    public void handleRead(SelectionKey key) {
        try {
            while (true) {
                ByteBuffer targetBB = packetHandler.getByteBuffer();
                assert targetBB.hasRemaining() : packetHandler.getClass().getName() + " at " + targetBB.position();
                int readBytes = socketChannel.read(targetBB);

                // no more data in system buffer
                if (readBytes == 0) {
                    return;
                }

                // EOF - that means that the other side close his socket, so we
                // should close this connection too.
                if (readBytes == -1) {
                    close();
                    return;
                }

                // if the whole packet was read, then notify packet handler
                if (!targetBB.hasRemaining()) {
                    packetHandler.finished();
                    return;
                }
            }
        } catch (IOException e) {
            logger.debug("Client connection faulted on read: {} ({})",
                    socketChannel.socket().getInetAddress(), e.getCause());
            close();
            return;
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread interrupted. Quitting.");
        }
    }

    /**
     * This method is called from selector thread to notify that there is free
     * space in system send buffer, and it is possible to send new packet of
     * data.
     */
    @Override
    public void handleWrite(SelectionKey key) {
        // The might have disconnected. In that case, discard the message
        if (!socketChannel.isOpen())
            return;

        try {
            socketChannel.write(message);
        } catch (IOException e) {
            logger.debug("Client connection faulted on write: {} ({})",
                    socketChannel.socket().getInetAddress(), e.getCause());
            close();
            return;
        }

        if (!message.hasRemaining()) {
            message = null;
            key.interestOpsAnd(~SelectionKey.OP_WRITE);
        }
    }

    /**
     * @param byteBuffer
     */
    public void sendBuffer(final ByteBuffer byteBuffer) {
        // discard message if channel is not connected
        if (!socketChannel.isConnected())
            return;

        // clients may communicate only in request-reply way; hence there must
        // be no other message to be sent
        assert message == null;

        try {
            socketChannel.write(byteBuffer);
        } catch (IOException e) {
            logger.debug("Client connection faulted on write: {} ({})",
                    socketChannel.socket().getInetAddress(), e.getCause());
            close();
            return;
        }

        if (!byteBuffer.hasRemaining())
            return;

        message = byteBuffer;

        // this will wake the selector and bid it to write the data
        selectorThread.scheduleAddChannelInterest(socketChannel, SelectionKey.OP_WRITE);
    }

    /**
     * Schedules a task to close the socket. Use when closing the socket from a
     * thread other than the Selector responsible for this connection.
     */
    public void forceClose() {
        try {
            socketChannel.close();
        } catch (IOException e) {
        }
    }

    /**
     * Closes the underlying socket channel. It closes channel immediately so it
     * should be called only from selector thread.
     */
    public void close() {
        assert selectorThread.amIInSelector();
        try {
            socketChannel.close();
        } catch (IOException e) {
            logger.error("Error closing socket", e);
        }
    }

    public SelectorThread getSelectorThread() {
        return selectorThread;
    }

    private final static Logger logger = LoggerFactory.getLogger(ReaderAndWriter.class);
}