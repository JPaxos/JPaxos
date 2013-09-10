package lsr.paxos.network;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import lsr.common.PID;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NioInputConnection extends NioConnection
{
    private static final int BUFFER_SIZE = 8192;

    private ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    private byte[] outputArray;
    private int outputArrayRemainging = 0;

    public NioInputConnection(NioNetwork network, PID replica, SocketChannel channel)
            throws IOException
    {
        super(network, replica, channel);

        channel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public void run()
    {
        String name = "NioInputConnection_" + replicaId + "->" + Network.localId;
        setName(name);
        logger.trace("starting {}", name);

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
                logger.debug("input connection problem with: {}", replicaId);
                // e.printStackTrace();
                disposeConnection();
                return;
            }
        }
    }

    // if (stop == true)
    // {
    // logger_.finest("quitting");
    // break;
    // }

    private void read(SelectionKey key) throws IOException
    {
        logger.trace("socket read: {}", replicaId);

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
                logger.debug("Received [{}] {} size: {}", replicaId, message,
                        message.byteSize());
                network.fireReceiveMessage(message, replicaId);

                outputArray = null;
            }
        }
        readBuffer.compact();
    }

    protected void disposeConnection()
    {
        for (SelectionKey key : selector.keys())
            key.cancel();
        logger.debug("input connection closed with: " + replicaId);
        network.removeConnection(replicaId, Network.localId);
    }

    private final static Logger logger = LoggerFactory.getLogger(Network.class);

}
