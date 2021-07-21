package lsr.common.nio;

import java.nio.ByteBuffer;

/**
 * Represents object responsible for handling packets. It provides
 * <code>ByteBuffer</code> which will be filled up with data by
 * <code>ReaderAndWriter</code>. When the entire buffer will be received, then
 * finished method is called by <code>ReaderAndWriter</code>. After finished()
 * exits, subsequent calls to getByteBuffer(); shall return a new buffer.
 * 
 * @see ByteBuffer
 * @see ReaderAndWriter
 */
public interface PacketHandler {
    /**
     * Returns byte buffer which should be filled with data to process it.
     * 
     * @return byte buffer
     */
    ByteBuffer getByteBuffer();

    /**
     * Process received packet of data. This is called after filling entire byte
     * buffer with data.
     * 
     * @throws InterruptedException
     */
    void finished() throws InterruptedException;
}