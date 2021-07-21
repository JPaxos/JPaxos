package lsr.paxos.replica;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.ClientCommand;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.nio.PacketHandler;
import lsr.common.nio.ReaderAndWriter;
import lsr.common.nio.SelectorThread;
import lsr.paxos.idgen.IdGenerator;
import lsr.paxos.idgen.IdGeneratorType;
import lsr.paxos.idgen.SimpleIdGenerator;
import lsr.paxos.idgen.TimeBasedIdGenerator;
import lsr.paxos.idgen.ViewEpochIdGenerator;
import lsr.paxos.storage.Storage;

/**
 * This class is used to handle one client connection. It uses
 * <code>ReaderAndWriter</code> for writing and reading packets from clients.
 * <p>
 * First it initializes client connection reading client id (or granting him a
 * new one). After successful initialization commands will be received, and
 * reply can be send to clients.
 * 
 * @see ReaderAndWriter
 */
public class NioClientProxy implements ClientProxy {
    private final ClientRequestManager requestManager;
    private long clientId;
    private ByteBuffer readBuffer = ByteBuffer.allocateDirect(
            processDescriptor.clientRequestBufferSize);
    private final ReaderAndWriter readerAndWriter;

    /** Generator for client IDs */
    public static IdGenerator idGenerator;

    /**
     * Creates new client proxy.
     * 
     * @param readerAndWriter - used to send and receive data from clients
     * @param requestManager - callback for executing command from clients
     * @param idGenerator - generator used to generate id's for clients
     */
    public NioClientProxy(ReaderAndWriter readerAndWriter, ClientRequestManager requestManager) {
        this.readerAndWriter = readerAndWriter;
        this.requestManager = requestManager;

        if (logger.isInfoEnabled())
            logger.info("New client connection: {}", readerAndWriter.socketChannel.socket());

        this.readerAndWriter.setPacketHandler(new InitializePacketHandler());
    }

    /**
     * Sends the reply to client held by this proxy. This method has to be
     * called after client is initialized.
     * 
     * @param clientReply - reply send to underlying client
     */
    public void send(final ClientReply clientReply) {
        readerAndWriter.getSelectorThread().beginInvoke(() -> {
            readerAndWriter.sendBuffer(clientReply.toByteBuffer());
        });
    }

    /**
     * executes command from byte buffer
     * 
     * @throws InterruptedException
     */
    private void execute(ByteBuffer buffer) throws InterruptedException {
        ClientCommand command = new ClientCommand(buffer);
        requestManager.onClientRequest(command, this);
    }

    /**
     * Waits for first byte, 'T' or 'F' which specifies whether we should grant
     * new id for this client, or it has one already.
     */
    private class InitializePacketHandler implements PacketHandler {

        public InitializePacketHandler() {
            readBuffer.clear();
            readBuffer.limit(1);
        }

        public void finished() {
            readBuffer.rewind();
            byte b = readBuffer.get();

            if (b == lsr.paxos.client.Client.REQUEST_NEW_ID) {
                readerAndWriter.setPacketHandler(new ClientCommandPacketHandler());

                // grant new id for client
                clientId = idGenerator.next();
                ByteBuffer bytesClientId = ByteBuffer.allocateDirect(8);
                bytesClientId.putLong(clientId);
                bytesClientId.flip();

                readerAndWriter.sendBuffer(bytesClientId);

            } else if (b == lsr.paxos.client.Client.HAVE_CLIENT_ID) {
                // wait for receiving id from client
                readerAndWriter.setPacketHandler(new ClientIdPacketHandler());
            } else {
                // command client is incorrect; close the underlying connection
                logger.error(
                        "Incorrect initialization header. Expected '{}' or '{}' but received {}",
                        lsr.paxos.client.Client.REQUEST_NEW_ID,
                        lsr.paxos.client.Client.HAVE_CLIENT_ID, b);
                readerAndWriter.forceClose();
            }
        }

        public ByteBuffer getByteBuffer() {
            return readBuffer;
        }
    }

    /**
     * Waits for the id from client. After receiving it starts receiving client
     * commands packets.
     */
    private class ClientIdPacketHandler implements PacketHandler {

        public ClientIdPacketHandler() {
            readBuffer.position(0);
            readBuffer.limit(8);
        }

        public void finished() {
            readBuffer.rewind();
            clientId = readBuffer.getLong();
            readerAndWriter.setPacketHandler(new ClientCommandPacketHandler());
        }

        public ByteBuffer getByteBuffer() {
            return readBuffer;
        }
    }

    /**
     * Waits for the header and then for the message from the client.
     */
    private class ClientCommandPacketHandler implements PacketHandler {

        private boolean header = true;

        public ClientCommandPacketHandler() {
            readBuffer.position(0);
            readBuffer.limit(ClientCommand.HEADERS_SIZE);
        }

        public void finished() throws InterruptedException {
            if (header) {
                int sizeOfValue = readBuffer.getInt(ClientCommand.HEADER_VALUE_SIZE_OFFSET);

                int headersAndMessageSize = ClientCommand.HEADERS_SIZE + sizeOfValue;
                if (headersAndMessageSize > readBuffer.capacity()) {
                    // if the request exceeds buffer capacity
                    ByteBuffer newBuffer = ByteBuffer.allocateDirect(headersAndMessageSize);
                    readBuffer.flip();
                    newBuffer.put(readBuffer);
                    readBuffer = newBuffer;
                }
                readBuffer.limit(headersAndMessageSize);
            } else {
                readBuffer.position(0);
                execute(readBuffer);
                readBuffer.position(0);
                readBuffer.limit(ClientCommand.HEADERS_SIZE);
            }
            header = !header;
        }

        public ByteBuffer getByteBuffer() {
            return readBuffer;
        }
    }

    public String toString() {
        return "client: " + clientId + " - " + readerAndWriter.socketChannel.socket().getPort();
    }

    public SelectorThread getSelectorThread() {
        return readerAndWriter.getSelectorThread();
    }

    public void closeConnection() {
        readerAndWriter.close();
    }

    public static void createIdGenerator(Storage storage) {
        IdGeneratorType igt;
        try {
            igt = IdGeneratorType.valueOf(processDescriptor.clientIDGenerator);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Unknown id generator: " +
                                       processDescriptor.clientIDGenerator + ". Valid options: " +
                                       Arrays.toString(IdGeneratorType.values()),
                    e);
        }

        switch (igt) {
            case Simple:
                idGenerator = new SimpleIdGenerator();
                break;
            case TimeBased:
                idGenerator = new TimeBasedIdGenerator();
                break;
            case ViewEpoch:
                idGenerator = new ViewEpochIdGenerator(storage.getRunUniqueId());
                break;

            default:
                throw new RuntimeException("Unknown id generator: " +
                                           processDescriptor.clientIDGenerator +
                                           ". Valid options: " +
                                           Arrays.toString(IdGeneratorType.values()));
        }

    }

    @Override
    public boolean redirectElsewhere() {
        send(new ClientReply(Result.RECONNECT, new byte[0]));
        return true;
    }

    private final static Logger logger = LoggerFactory.getLogger(NioClientProxy.class);

}
