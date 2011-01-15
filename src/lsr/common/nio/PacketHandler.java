package lsr.common.nio;

import java.nio.ByteBuffer;

/**
 * Represents object responsible for handling one packet. It provides
 * <code>ByteBuffer</code> which will be filled up with data by
 * <code>ReaderAndWriter</code>. When the entire buffer will be received, then
 * finished method is called by <code>ReaderAndWriter</code>.
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
     */
    void finished();
}