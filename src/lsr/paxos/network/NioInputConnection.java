package lsr.paxos.network;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.PID;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

public class NioInputConnection extends NioConnection
{
    private final static Logger logger = Logger.getLogger(Network.class
        .getCanonicalName());

    private static final int BUFFER_SIZE = 8192;

    private ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    private byte[] outputArray;
    private int outputArrayRemainging = 0;

    public NioInputConnection(NioNetwork network, PID replica, SocketChannel channel) throws IOException
    {
        super(network, replica, channel);

        channel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public void run()
    {
        setName("NioInputConnection_" + replica.getId() + "->" + Network.localId);
        logger.finest("starting");
        while (true)
        {
            try
            {
                selector.select();

                Iterator<SelectionKey> selectedKeys = this.selector
                    .selectedKeys().iterator();
                while (selectedKeys.hasNext())
                {
                    SelectionKey key = (SelectionKey) selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid())
                        continue;

                    if (key.isReadable())
                        read(key);
                }
            } catch (Exception e)
            {
                logger.finest("input connection problem with: " + replica.getId());
                // e.printStackTrace();
                disposeConnection();
                return;
            }
        }
    }

    // if (stop == true)
    // {
    // logger.finest("quitting");
    // break;
    // }

    private void read(SelectionKey key) throws IOException
    {
        logger.finest("socket read: " + replica.getId());

        int numRead;
        try
        {
            numRead = channel.read(readBuffer);
        } catch (IOException e)
        {
            disposeConnection();
            return;
        }

        if (numRead == -1)
        {
            disposeConnection();
            return;
        }

        // Network.bytesRead.add(peerId, numRead);
        // Network.numberOfReads.increment(peerId);

        readBuffer.flip();

        while (readBuffer.remaining() > 0)
        {
            if (outputArray == null)
            {
                if (readBuffer.remaining() < 4)
                {
                    break;
                }
                outputArrayRemainging = readBuffer.getInt();
                outputArray = new byte[outputArrayRemainging];
                continue;
            }

            int bytesToCopy = Math.min(outputArrayRemainging, readBuffer.remaining());
            readBuffer.get(outputArray, outputArray.length
                                        - outputArrayRemainging, bytesToCopy);
            outputArrayRemainging -= bytesToCopy;

            if (outputArrayRemainging == 0)
            {
                // network.fireReceiveMessage(Message.create(outputArray,
                // MessageType.A, false), peerId);
     
                // TODO poprawic strumienie
                DataInputStream input = new DataInputStream(new ByteArrayInputStream(outputArray));
                Message message = MessageFactory.create(input);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Received [" + replica.getId() + "] " + message +
                                " size: " + message.byteSize());
                }
                network.fireReceiveMessage(message, replica.getId());

                outputArray = null;
            }
        }
        readBuffer.compact();
    }

    protected void disposeConnection()
    {
        for (SelectionKey key : selector.keys())
            key.cancel();
        logger.fine("input connection closed with: " + replica.getId());
        network.removeConnection(replica.getId(), Network.localId);
    }
}
