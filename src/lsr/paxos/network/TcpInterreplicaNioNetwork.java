package lsr.paxos.network;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.EOFException;
import java.io.IOException;
import java.lang.Thread.State;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Pipe;
import java.nio.channels.Pipe.SinkChannel;
import java.nio.channels.Pipe.SourceChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.BitSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.KillOnExceptionHandler;
import lsr.common.PID;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

public class TcpInterreplicaNioNetwork extends Network implements Runnable {

    /// handles incoming data and dispatching writes
    protected final Thread worker = new Thread(this, "TcpInterreplicaNioNetwork");

    /// number of replicas
    protected final int n = processDescriptor.numReplicas;

    /*
     * Approximately this many previous messages are are kept in outgoing
     * buffers when no connection to a replica is available.
     * 
     * It is beneficial to send such a backlog upon reconnect / startup /
     * recovery, as the system stabilizes faster
     */
    public static final int outBuffersMaxBacklogCount = 128;

    /// holds the preferred connection between this replica and a peer
    protected final SocketChannel[] liveConnection = new SocketChannel[n];
    /// holds the preferred connection key between this replica and a peer
    protected final SelectionKey[] liveConnectionKeys = new SelectionKey[n];

    /// If only a part of the message has been sent, this is set
    protected final SocketChannel[] partialWriteConnection = new SocketChannel[n];

    /// holds attempts of this replica to connect to others
    protected final SocketChannel[] myAtteptToConnect = new SocketChannel[n];
    /// holds incoming connections from others to this replica
    protected final SocketChannel[] incomingConnection = new SocketChannel[n];

    /// listens for incoming connection from peer replicas
    protected final ServerSocketChannel listeningSocket;
    /// nio is missing timers; hence, we use a timer that sends data to pipe
    protected final SinkChannel retryConnectionPipe;
    protected final AbstractSelector selector;

    @SuppressWarnings("unchecked")
    /// data to be sent to a target peer
    protected final ConcurrentLinkedQueue<ByteBuffer>[] outBuffers = new ConcurrentLinkedQueue[n];
    /// approximate information if there is a connection to the peer replica
    AtomicBoolean[] isLive = new AtomicBoolean[n];
    /// approximate count of msgs in outBuffers; valid iff !isLive
    protected final AtomicInteger[] outBuffersSize = new AtomicInteger[n];

    {
        for (int id = (localId != 0 ? 0 : 1); id < n; id += ((id + 1 == localId) ? 2 : 1)) {
            outBuffers[id] = new ConcurrentLinkedQueue<ByteBuffer>();
            isLive[id] = new AtomicBoolean(false);
            outBuffersSize[id] = new AtomicInteger(0);
        }
    }

    public TcpInterreplicaNioNetwork() {
        try {
            selector = SelectorProvider.provider().openSelector();
            listeningSocket = SelectorProvider.provider().openServerSocketChannel();
            Pipe pipe = SelectorProvider.provider().openPipe();
            SourceChannel source = pipe.source();
            source.configureBlocking(false);
            source.register(selector, SelectionKey.OP_READ).attach(new handleRetryRequest(source));
            retryConnectionPipe = pipe.sink();

        } catch (IOException e) {
            throw new RuntimeException("Java is mean. No selector/ssc granted!", e);
        }
    }

    @Override
    public void start() {
        assert (worker.getState() == State.NEW);
        worker.start();
    }

    protected ByteBuffer packMessageToBB(Message message) {
        ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES + message.byteSize());
        bb.putInt(message.byteSize());
        message.writeTo(bb);
        assert !bb.hasRemaining();
        bb.flip();
        return bb;
    }

    @Override
    protected void send(Message message, int destination) {
        ByteBuffer bb = packMessageToBB(message);
        if (queueSend(bb, destination))
            selector.wakeup();
    }

    @Override
    protected void send(Message message, BitSet /* const */ destinations) {
        ByteBuffer bb = packMessageToBB(message);
        boolean shouldWake = false;
        for (int it = destinations.nextSetBit(0); it >= 0; it = destinations.nextSetBit(it + 1))
            shouldWake |= queueSend(bb.duplicate(), it);
        if (shouldWake)
            selector.wakeup();
    }

    protected boolean queueSend(ByteBuffer buffer, int destination) {
        // TODO (JK): the isLive.get below should be as fast as possible, and
        // both false-positives and false-negatives are fine as long as they can
        // happen for a bounded time.
        if (!isLive[destination].get()) {
            // if there is no connection:

            // put the new message
            outBuffers[destination].offer(buffer);

            // TODO (JK): in a corner case this can deadlock - if connection is
            // established in parallel, we don't wake up the selector. For now,
            // don't care: selector is frequently woken

            if (outBuffersSize[destination].get() <= outBuffersMaxBacklogCount)
                // there is still place; just increment counter
                // only as many threads as there are in the system can get here,
                // so the size of the buffer is bounded to
                // (outBuffersBacklogCount + #threads)
                outBuffersSize[destination].incrementAndGet();
            else
                // remove an old message
                outBuffers[destination].poll();

            return false;
        }
        boolean wasEmpty = outBuffers[destination].isEmpty();
        outBuffers[destination].offer(buffer);
        return wasEmpty;
    }

    protected void handleWrite(int id) {
        assert !outBuffers[id].isEmpty();
        ByteBuffer buffer = outBuffers[id].peek();
        do {
            try {
                liveConnection[id].write(buffer);
            } catch (IOException e) {
                handleConnectionError(liveConnection[id], id, e, true);
                return;
            }
            if (buffer.hasRemaining()) {
                // wait for write
                liveConnectionKeys[id].interestOpsOr(SelectionKey.OP_WRITE);
                return;
            }
            outBuffers[id].poll();
        } while ((buffer = outBuffers[id].peek()) != null);

        // wrote all; don't wait for write
        liveConnectionKeys[id].interestOpsAnd(~SelectionKey.OP_WRITE);
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new KillOnExceptionHandler());

        try {
            // setup
            listeningSocket.configureBlocking(false);
            SelectionKey listeningKey = listeningSocket.register(selector, SelectionKey.OP_ACCEPT);
            listeningKey.attach(new handleAccept());
            int myPort = processDescriptor.config.getProcess(localId).getReplicaPort();
            listeningSocket.bind(new InetSocketAddress(myPort), Math.min(n, 10));

            logger.debug("TcpInterreplicaNioNetwork started");

            for (int id = (localId != 0 ? 0 : 1); id < n; id += ((id + 1 == localId) ? 2 : 1))
                retryConnection(id);
        } catch (IOException e) {
            throw new RuntimeException("Setting up TcpInterreplicaNioNetwork failed", e);
        }

        // main l∞p
        while (true) {
            try {
                selector.select();
            } catch (IOException e) {
                throw new RuntimeException("Selector selected an error for you", e);
            }

            // writes
            for (int id = (localId != 0 ? 0 : 1); id < n; id += ((id + 1 == localId) ? 2 : 1)) {
                // no data to write
                if (outBuffers[id].isEmpty())
                    continue;

                // no connection
                if (liveConnectionKeys[id] == null || !liveConnectionKeys[id].isValid())
                    continue;

                boolean writeWanted = 0 == (liveConnectionKeys[id].interestOps() &
                                            SelectionKey.OP_WRITE);
                boolean writeFired = 0 == (liveConnectionKeys[id].readyOps() &
                                           SelectionKey.OP_WRITE);

                // cannot write yet
                if (writeWanted && !writeFired)
                    continue;

                handleWrite(id);
            }

            // reads, connects, accepts, killer pipe
            for (SelectionKey key : selector.selectedKeys()) {
                assert key.attachment() != null : key;
                ((Runnable) key.attachment()).run();
            }

            selector.selectedKeys().clear();
        }
    }

    void retryConnection(int id) {
        assert Thread.currentThread() == worker;

        PID process = processDescriptor.config.getProcess(id);
        InetSocketAddress target = new InetSocketAddress(process.getHostname(),
                process.getReplicaPort());

        logger.debug("Attempting to establish connection to p{}", id);

        if (myAtteptToConnect[id] != null) {
            try {
                myAtteptToConnect[id].close();
            } catch (IOException ex) {
                // Deliberately ignore this error
            }
        }

        try {
            SocketChannel sc = SelectorProvider.provider().openSocketChannel();
            myAtteptToConnect[id] = sc;
            sc.configureBlocking(false);
            SelectionKey key = sc.register(selector, SelectionKey.OP_CONNECT);
            key.attach(new handleConnect(sc, key, id));
            sc.connect(target);
        } catch (IOException e) {
            throw new RuntimeException("Connectablility lost", e);
        }
    }

    public void choosePreferredConnection(int id) {
        assert Thread.currentThread() == worker;

        SocketChannel oldLive = liveConnection[id];

        SocketChannel pref;
        SocketChannel back;

        if (id < localId) {
            pref = myAtteptToConnect[id];
            back = incomingConnection[id];
        } else {
            pref = incomingConnection[id];
            back = myAtteptToConnect[id];
        }

        if ((pref == null || !pref.isConnected()) && (back == null || !back.isConnected())) {
            outBuffersSize[id].set(outBuffers[id].size());
            isLive[id].set(false);
            liveConnection[id] = null;
            liveConnectionKeys[id] = null;
            logger.info("There is no connection to p" + id);

            int overflowBacklogCount = outBuffersSize[id].get() - outBuffersMaxBacklogCount;
            if (overflowBacklogCount > 0) {
                // limit backlog
                outBuffersSize[id].addAndGet(-overflowBacklogCount);
                while (--overflowBacklogCount > 0)
                    outBuffers[id].remove();
            } else {
                // make first message be sent as whole
                ByteBuffer first = outBuffers[id].peek();
                if (first != null)
                    first.rewind();
            }

            return;
        }
        isLive[id].set(true);

        if (pref != null && pref.isConnected()) {
            liveConnection[id] = pref;
            SelectionKey prefKey = pref.keyFor(selector);
            liveConnectionKeys[id] = prefKey;

            if (back != null && back.isConnected()) {
                logger.info("There are two connections to p{}", id);
            } else {
                logger.info("There is one connection to p{} (preffered)", id);
            }
        } else {
            liveConnection[id] = back;
            SelectionKey backKey = back.keyFor(selector);
            liveConnectionKeys[id] = backKey;
            logger.info("There is one connection to p{} (backup)", id);
        }

        // unlikely paths.
        if (oldLive != liveConnection[id]) {
            // we already sent part of a message to live and this method fires
            ByteBuffer bb = outBuffers[id].peek();
            if (bb != null && bb.position() != 0) {
                assert oldLive != null;
                if (oldLive.isConnected()) {
                    // if back is live, then send the rest to it
                    outBuffers[id].poll();
                    SelectionKey oldKey = oldLive.keyFor(selector);
                    handleIO oldIo = (handleIO) oldKey.attachment();
                    oldIo.addPartialBufferToWrite(bb);
                } else {
                    // else re-send whole msg
                    bb.position(0);
                }
            }

            // if the preferred died before back sent all that was stashed in
            // the lines above, then we need to put it back on the send queue.
            // This is extremely unlikely.
            Object attachment = liveConnectionKeys[id].attachment();
            if (attachment instanceof handleIO) {
                // attachment can be instanceof handleHello
                handleIO io = (handleIO) attachment;
                ByteBuffer staleData = io.removePartialBufferToWrite();
                if (staleData != null) {
                    outBuffers[id].add(staleData);
                    while (outBuffers[id].peek() != staleData) {
                        outBuffers[id].add(outBuffers[id].poll());
                    }
                }
            }
        }
    }

    private void handleConnectionError(SocketChannel sc, int id, IOException e,
                                       boolean removeFromSelected) {
        // make sure that if write failed, then we're not going to attempt a
        // read form this channel
        if (removeFromSelected)
            selector.selectedKeys().remove(sc.keyFor(selector));

        boolean issueReconnect = false;

        // erase that connection
        if (sc == myAtteptToConnect[id]) {
            issueReconnect = true;
            myAtteptToConnect[id] = null;
            logger.info("Connection me -> p" + id + " died ({})", e.getMessage());
        } else {
            assert sc == incomingConnection[id];
            incomingConnection[id] = null;
            logger.info("Connection me <- p" + id + " died ({})", e.getMessage());
        }

        // attempt to close the connection
        try {
            sc.close();
        } catch (IOException e1) {
            // ignore
        }

        // if we have a backup, then try to use it
        choosePreferredConnection(id);

        // if this was an outgoing connection, then try to re-establish
        if (issueReconnect)
            retryConnection(id);
    }

    final class handleAccept implements Runnable {
        public void run() {
            assert listeningSocket.keyFor(selector).isAcceptable();

            SocketChannel incoming = null;
            SelectionKey key;
            try {
                incoming = listeningSocket.accept();
            } catch (IOException e) {
                throw new RuntimeException("Acceptance lost", e);
            }

            assert incoming != null;

            try {
                incoming.configureBlocking(false);
                incoming.setOption(StandardSocketOptions.TCP_NODELAY, true);
            } catch (IOException e) {
                throw new RuntimeException("Socket is bananas", e);
            }
            try {
                key = incoming.register(selector, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                logger.info("Reading from a newly accepted socket failed", e);
                return;
            }
            try {
                logger.debug("New connection on replica port from {}", incoming.getRemoteAddress());
            } catch (IOException e) {
                logger.debug("New connection on replica port from unknown location ({})",
                        e.getMessage());
            }
            key.attach(new handleHello(incoming, key));
        }
    }

    final class handleHello implements Runnable {

        private final SocketChannel incoming;
        private final SelectionKey key;

        public handleHello(SocketChannel incoming, SelectionKey key) {
            this.incoming = incoming;
            this.key = key;
        }

        public void run() {
            ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
            int id;
            try {
                int read = incoming.read(bb);
                if (read != Integer.BYTES)
                    throw new IOException(
                            "Could not read replica ID - got less than 4 bytes (got " + read +
                                          " bytes)");
                bb.flip();
                id = bb.getInt();
                if (id < 0 || id >= n || id == localId) {
                    throw new IOException("Got incorrect ID (" + id + ")");
                }
            } catch (IOException e) {
                logger.info("Incoming connection faulted before setting up", e);
                try {
                    incoming.close();
                } catch (IOException e1) {
                }
                return;
            }

            logger.info("Replica {} connected", id);
            incomingConnection[id] = incoming;

            key.attach(new handleIO(incoming, key, id));

            choosePreferredConnection(id);
        }

    }

    class handleRetryRequest implements Runnable {
        private final ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
        private final SourceChannel source;

        public handleRetryRequest(SourceChannel source) {
            this.source = source;
        }

        public void run() {
            try {
                source.read(bb);
            } catch (IOException e) {
                throw new RuntimeException("Source dried");
            }
            bb.flip();
            int id = bb.getInt();
            bb.flip();
            retryConnection(id);
        }
    }

    protected final Timer javaNioIsMissingTimers = new Timer(true);

    final class handleConnect implements Runnable {
        private final int id;
        private final SocketChannel sc;
        private final SelectionKey key;

        TimerTask killer = new TimerTask() {
            public void run() {
                logger.debug("Timed out connecting to {}", id);
                // writes victim id to a pipe that is registered in selector
                ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
                bb.putInt(id);
                bb.flip();
                try {
                    retryConnectionPipe.write(bb);
                } catch (IOException e) {
                    throw new RuntimeException("You hear clanking from the pipes...");
                }
            }
        };

        public handleConnect(SocketChannel sc, SelectionKey key, int id) {
            this.sc = sc;
            this.key = key;
            this.id = id;
            javaNioIsMissingTimers.schedule(killer, processDescriptor.tcpReconnectTimeout);
        }

        public void run() {
            assert key.isConnectable();

            boolean success;
            try {
                success = sc.finishConnect();
            } catch (IOException e) {
                logger.info("Connection to {} failed ({})", id, e.getMessage());
                try {
                    sc.close();
                } catch (IOException f) {
                }
                return;
            }
            assert success;

            logger.info("Connected to replica {}", id);

            ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
            bb.putInt(localId);
            bb.flip();
            try {
                sc.write(bb);
            } catch (IOException e) {
                logger.info("Writing to newly connected socket failed", e);
                try {
                    sc.close();
                } catch (IOException f) {
                }
                return;
            }
            assert !bb.hasRemaining();

            try {
                sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
            } catch (IOException e) {
                logger.warn("Could not set TCP_NODELAY", e);
            }

            key.interestOps(SelectionKey.OP_READ);

            killer.cancel();

            key.attach(new handleIO(sc, key, id));

            choosePreferredConnection(id);
        }
    }

    final class handleIO implements Runnable {

        ByteBuffer bb = ByteBuffer.allocateDirect(2 * 1024 * 1024);
        int bbMyMark = 0;

        final SocketChannel sc;
        final SelectionKey key;
        final int id;

        boolean readingSize = true;
        int msgSize;

        public handleIO(SocketChannel sc, SelectionKey key, int id) {
            this.sc = sc;
            this.key = key;
            this.id = id;
        }

        private void moveBbContentsFromEndToBegin() {
            bb.limit(bb.position());
            bb.position(bbMyMark);
            bb.compact();
            bbMyMark = 0;
        }

        // this is used iff preferred connection changes
        private ByteBuffer messageLastChunk = null;

        protected void addPartialBufferToWrite(ByteBuffer bb) {
            messageLastChunk = bb;
        }

        protected ByteBuffer removePartialBufferToWrite() {
            ByteBuffer bb = messageLastChunk;
            messageLastChunk = null;
            return bb;
        }

        @Override
        public void run() {
            if (messageLastChunk != null && key.isWritable()) {
                // this is an unlikely branch when a preferred connection
                // appeared while half of a message was sent in the backup one,
                // and we need to send the remaining chunk in backup
                try {
                    sc.write(messageLastChunk);
                } catch (IOException e) {
                    handleConnectionError(sc, id, e, false);
                    return;
                }
                if (!messageLastChunk.hasRemaining()) {
                    key.interestOpsAnd(~SelectionKey.OP_WRITE);
                }
            }

            if (!key.isReadable())
                return;

            int bytesRead;
            try {
                bytesRead = sc.read(bb);
                if (bytesRead == -1)
                    throw new EOFException();
            } catch (IOException e) {
                handleConnectionError(sc, id, e, false);
                return;
            }

            while (true) {
                int bytesReady = bb.position() - bbMyMark;

                if (readingSize) {
                    if (bytesReady < Integer.BYTES) {
                        // got only a part of the message size
                        if (bb.capacity() - bbMyMark < Integer.BYTES)
                            moveBbContentsFromEndToBegin();
                        return;
                    }
                    readingSize = false;

                    msgSize = bb.getInt(bbMyMark);
                    bbMyMark += Integer.BYTES;

                    // resize buffer if needed
                    if (msgSize > bb.capacity() / 2) {
                        long newSize = msgSize * 2L;
                        if (newSize > Integer.MAX_VALUE)
                            newSize = Integer.MAX_VALUE;
                        ByteBuffer newBB = ByteBuffer.allocateDirect((int) newSize);
                        bb.limit(bb.position());
                        bb.position(bbMyMark);
                        newBB.put(bb);
                        assert !bb.hasRemaining();
                        bb = newBB;
                        bbMyMark = 0;
                    }
                } else {
                    if (bytesReady < msgSize) {
                        // got only a part of the message
                        if (bb.capacity() - bbMyMark < msgSize) {
                            // message is at the end of the buffer an it won't
                            // fit; move it to the beginning
                            moveBbContentsFromEndToBegin();
                        }
                        return;
                    }
                    readingSize = true;

                    ByteBuffer tempBB = bb.duplicate();
                    tempBB.position(bbMyMark);
                    bbMyMark += msgSize;
                    tempBB.limit(bbMyMark);

                    Message message = MessageFactory.create(tempBB);
                    assert !tempBB.hasRemaining() : "Serialisation error of: " +
                                                    message.toString() + " remaining: " +
                                                    tempBB.remaining();

                    fireReceiveMessage(message, id);
                }
            }
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(TcpInterreplicaNioNetwork.class);
}
