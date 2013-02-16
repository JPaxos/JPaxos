package lsr.common.nio;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    /** lock for tasks object; tasks cannot be used, as it is recreated often */
    private final Object taskLock = new Object();

    /** list of active tasks waiting for execution in selector thread */
    private List<Runnable> tasks = new ArrayList<Runnable>();

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
        logger.info("Selector started.");

        // run main loop until thread is interrupted
        while (!Thread.interrupted()) {
            runScheduleTasks();

            try {
                // FIXME: JK investigate if it is better to poll the tasks or to
                // use event-driven approach. Now polling is made, the previous
                // comment say it's better

                int selectedCount = selector.select(10);

                if (selectedCount > 0) {
                    processSelectedKeys();
                }

            } catch (IOException e) {
                // it shouldn't happen in normal situation so print stack trace
                // and kill the application
                logger.log(Level.SEVERE, "Unexpected exception", e);
                closeSelectorThread();
                System.exit(1);
            }
        }
    }

    /**
     * Processes all keys currently selected by selector. Ready interest set is
     * always erased before handling it, so handler has to renew its interest.
     */
    private void processSelectedKeys() {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            // remove the selected key to not process it twice
            SelectionKey key = it.next();
            it.remove();

            // erase flags of ready operation
            key.interestOps(key.interestOps() & ~key.readyOps());

            if (key.isAcceptable()) {
                ((AcceptHandler) key.attachment()).handleAccept();
                if (!key.isValid())
                    continue;
            }
            if (key.isReadable()) {
                ((ReadWriteHandler) key.attachment()).handleRead();
                if (!key.isValid())
                    continue;
            }
            if (key.isWritable()) {
                ((ReadWriteHandler) key.attachment()).handleWrite();
                if (!key.isValid())
                    continue;
            }
            if (key.isConnectable()) {
                ((ConnectHandler) key.attachment()).handleConnect();
            }
        }
    }

    /**
     * Invokes specified task asynchronously in <code>SelectorThread</code>. The
     * methods returns immediately.
     * 
     * @param task - task to run in <code>SelectorThread</code>
     */
    public void beginInvoke(Runnable task) {
        synchronized (taskLock) {
            tasks.add(task);
            // Do not wakeup the Selector thread by calling selector.wakeup().
            // Instead, the selector will periodically poll the array with tasks
            // // selector.wakeup();
        }
    }

    /**
     * Sets the interest set of specified channel(the old interest will be
     * erased). This method can be called from any thread.
     * 
     * @param channel - the channel to change interest set for
     * @param operations - new interest set
     */
    public void scheduleSetChannelInterest(final SelectableChannel channel, final int operations) {
        // Minimize locking time: create the object outside the critical
        // section.
        Runnable task = new Runnable() {
            public void run() {
                setChannelInterest(channel, operations);
            }
        };

        synchronized (taskLock) {
            tasks.add(task);
        }
    }

    /**
     * Sets the interest set of specified channel (the old interest will be
     * erased). This method has to be call from <code>SelectorThread</code>.
     * 
     * @param channel - the channel to change interest set for
     * @param operations - new interest set
     */
    public void setChannelInterest(SelectableChannel channel, int operations) {
        assert this == Thread.currentThread() : "Method not called from selector thread";

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(operations);
        }
    }

    /**
     * Adds the interest set to specified channel. This method can be called
     * from any thread.
     * 
     * @param channel - the channel to add interest set for
     * @param operations - new interest set
     */
    public void scheduleAddChannelInterest(final SocketChannel channel, final int operations) {
        beginInvoke(new Runnable() {
            public void run() {
                addChannelInterest(channel, operations);
            }
        });
    }

    /**
     * Adds the interest set to specified channel. This method has to be call
     * from <code>SelectorThread</code>.
     * 
     * @param channel - the channel to add interest set for
     * @param operations - new interest set
     */
    public void addChannelInterest(SelectableChannel channel, int operations) {
        assert this == Thread.currentThread() : "Method not called from selector thread";

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | operations);
        }
    }

    /**
     * Removes the interest set from specified channel. This method can be
     * called from any thread.
     * 
     * @param channel - the channel to remove interest set for
     * @param operations - interests to remove
     */
    public void scheduleRemoveChannelInterest(final SocketChannel channel, final int operations) {
        beginInvoke(new Runnable() {
            public void run() {
                removeChannelInterest(channel, operations);
            }
        });
    }

    /**
     * Removes the interest set from specified channel. This method has to be
     * call from <code>SelectorThread</code>.
     * 
     * @param channel - the channel to remove interest set for
     * @param operations - interests to remove
     */
    public void removeChannelInterest(SelectableChannel channel, int operations) {
        assert this == Thread.currentThread() : "Method not called from selector thread";

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() & ~operations);
        }
    }

    /**
     * Registers specified channel and handler to underlying selector. This
     * method has to be call from <code>SelectorThread</code>.
     * 
     * @param channel - channel to register in selector
     * @param operations - the initial interest operations for channel
     * @param handler - notified about every ready operation on channel
     * 
     * @throws IOException if an I/O error occurs
     */
    public void scheduleRegisterChannel(final SelectableChannel channel, final int operations,
                                        final Object handler) {

        beginInvoke(new Runnable() {
            public void run() {
                try {
                    registerChannel(channel, operations, handler);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        });
    }

    /**
     * Registers specified channel and handler to underlying selector. This
     * method can only be called from selector thread.
     * 
     * @param channel - channel to register in selector
     * @param operations - the initial interest operations for channel
     * @param handler - notified about every ready operation on channel
     * 
     * @throws IOException if an I/O error occurs
     */
    public void registerChannel(SelectableChannel channel, int operations, Object handler)
            throws IOException {
        assert this == Thread.currentThread() : "Method not called from selector thread";

        if (!channel.isOpen()) {
            throw new IOException("Channel is closed");
        }

        if (channel.isRegistered()) {
            SelectionKey key = channel.keyFor(selector);
            assert key != null : "The channel is not registered to selector?";
            key.interestOps(operations);
            key.attach(handler);
        } else {
            channel.configureBlocking(false);
            channel.register(selector, operations, handler);
        }
    }

    /** Runs all schedule tasks in selector thread. */
    private void runScheduleTasks() {
        // To minimize the time the lock is held, make a copy of the array
        // with the tasks while holding the lock then release the lock and
        // execute the tasks
        List<Runnable> tasksCopy;
        synchronized (taskLock) {
            if (tasks.isEmpty()) {
                return;
            }
            tasksCopy = tasks;
            tasks = new ArrayList<Runnable>(4);
        }
        for (Runnable task : tasksCopy) {
            task.run();
        }
    }

    private void closeSelectorThread() {
        try {
            selector.close();
        } catch (IOException e) {
            // it shouldn't happen
            logger.log(Level.SEVERE, "Unexpected exception", e);
            System.exit(1);
        }
    }

    public boolean amIInSelector() {
        return Thread.currentThread() == this;
    }

    private final static Logger logger = Logger.getLogger(SelectorThread.class.getCanonicalName());
}
