package lsr.common.nio;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.LoggerFactory;

import lsr.common.KillOnExceptionHandler;

/**
 * This class handles all keys registered in underlying selector. It is possible
 * to register new channels and changing interests in it.
 * <p>
 * Most methods has two version: normal and scheduled. Normal methods can only
 * be called from this thread. To invoke method from other thread, scheduled
 * version should be used.
 * 
 * @see Selector
 */
public final class SelectorThread extends Thread {
    private final Selector selector;

    /** list of active tasks waiting for execution in selector thread */
    private ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();

    /**
     * Initializes new thread responsible for handling channels.
     * 
     * @throws IOException if an I/O error occurs
     */
    public SelectorThread(int i) throws IOException {
        super("ClientIO-" + i);
        setDaemon(true);
        setDefaultUncaughtExceptionHandler(new KillOnExceptionHandler());
        selector = Selector.open();
    }

    /**
     * main loop which process all active keys in selector
     */
    public void run() {
        LoggerFactory.getLogger(SelectorThread.class).info("Selector started.");

        // run main loop until thread is interrupted
        while (!Thread.interrupted()) {

            try {
                int selectedCount = selector.select();

                runScheduleTasks();

                if (selectedCount > 0) {
                    processSelectedKeys();
                }

            } catch (IOException e) {
                throw new RuntimeException("Client selector faulted", e);
            }
        }
    }

    /**
     * Processes all keys currently selected by selector. Ready interest set is
     * always erased before handling it, so handler has to renew its interest.
     */
    private void processSelectedKeys() {
        for (SelectionKey key : selector.selectedKeys()) {
            try {
                if (key.isReadable())
                    ((ReadWriteHandler) key.attachment()).handleRead(key);
                if (key.isWritable())
                    ((ReadWriteHandler) key.attachment()).handleWrite(key);
                if (key.isAcceptable())
                    ((AcceptHandler) key.attachment()).handleAccept(key);
            } catch (CancelledKeyException ex) {
                // ignore the error
            }
        }
        selector.selectedKeys().clear();
    }

    /**
     * Invokes specified task asynchronously in <code>SelectorThread</code>. The
     * methods returns immediately.
     * 
     * @param task - task to run in <code>SelectorThread</code>
     */
    public void beginInvoke(Runnable task) {
        tasks.add(task);
        selector.wakeup();
    }

    /**
     * Sets the interest set of specified channel(the old interest will be
     * erased). This method can be called from any thread.
     * 
     * @param channel - the channel to change interest set for
     * @param operations - new interest set
     */
    public void scheduleSetChannelInterest(final SelectableChannel channel, final int operations) {
        assert !amIInSelector();
        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(operations);
        }
        selector.wakeup();
    }

    /**
     * Adds the interest set to specified channel. This method can be called
     * from any thread.
     * 
     * @param channel - the channel to add interest set for
     * @param operations - new interest set
     */
    public void scheduleAddChannelInterest(final SocketChannel channel, final int operations) {
        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOpsOr(operations);
        }

        if (!amIInSelector())
            selector.wakeup();
    }

    /**
     * Removes the interest set from specified channel. This method can be
     * called from any thread.
     * 
     * @param channel - the channel to remove interest set for
     * @param operations - interests to remove
     */
    public void scheduleRemoveChannelInterest(final SocketChannel channel, final int operations) {
        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() & ~operations);
        }

        if (!amIInSelector())
            selector.wakeup();
    }

    /**
     * Registers specified channel and handler to underlying selector.
     * 
     * @param channel - channel to register in selector
     * @param operations - the initial interest operations for channel
     * @param handler - notified about every ready operation on channel
     * @return
     * 
     * @throws IOException if an I/O error occurs
     */
    public SelectionKey scheduleRegisterChannel(final SelectableChannel channel,
                                                final int operations,
                                                final Object handler)
            throws IOException {

        channel.configureBlocking(false);
        SelectionKey key = channel.register(selector, operations, handler);

        if (!amIInSelector())
            selector.wakeup();

        return key;

    }

    /** Runs all schedule tasks in selector thread. */
    private void runScheduleTasks() {
        while (true) {
            Runnable r = tasks.poll();
            if (r == null)
                return;
            r.run();
        }
    }

    public boolean amIInSelector() {
        return Thread.currentThread() == this;
    }
}
