package lsr.common;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Adds debugging functionality to the standard
 * {@link ScheduledThreadPoolExecutor}. The additional debugging support
 * consists of naming the thread used by the executor and checking if the
 * current thread executing is the executor thread. It also limits the number of
 * threads on the pool to one.
 * 
 * @author Nuno Santos (LSR)
 */
public class SingleThreadDispatcher extends ScheduledThreadPoolExecutor {
    private final NamedThreadFactory ntf;

    /**
     * Thread factory that names the thread and keeps a reference to the last
     * thread created. Intended for debugging.
     * 
     * @author Nuno Santos (LSR)
     */
    private final static class NamedThreadFactory implements ThreadFactory {
        final String name;
        private Thread lastCreatedThread;

        public NamedThreadFactory(String name) {
            this.name = name;
        }

        public Thread newThread(Runnable r) {
            // Name the thread and save a reference to it for debugging
            lastCreatedThread = new Thread(r, name);
            return lastCreatedThread;
        }
    }

    public SingleThreadDispatcher(String threadName) {
        super(1, new NamedThreadFactory(threadName));
        ntf = (NamedThreadFactory) getThreadFactory();
    }

    /**
     * Checks whether current thread is the same as the thread associated with
     * this dispatcher.
     * 
     * @return true if the current and dispatcher threads are equals, false
     *         otherwise
     */
    public boolean amIInDispatcher() {
        return Thread.currentThread() == ntf.lastCreatedThread;
    }

    public void checkInDispatcher() {
        assert amIInDispatcher() : "Wrong thread: " + Thread.currentThread().getName();
    }

    /**
     * If the current thread is the dispatcher thread, executes the task
     * directly, otherwise hands it over to the dispatcher thread and wait for
     * the task to be finished.
     * 
     * @param task - the task to execute
     */
    public void executeAndWait(Runnable task) {
        if (amIInDispatcher()) {
            task.run();
        } else {
            Future<?> future = submit(task);
            // Wait until the task is executed
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Handles exceptions thrown by the executed tasks. Kills the process on
     * exception as tasks shouldn't throw exceptions under normal conditions.
     */
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        /*
         * If the task is wrapped on a Future, any exception will be stored on
         * the Future and t will be null
         */
        if (r instanceof Future<?>) {
            Future<?> ft = (Future<?>) r;
            try {
                ft.get(0, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                Throwable e1 = (e.getCause() == null) ? e : e.getCause();
                e1.printStackTrace();
                logger.log(Level.SEVERE, "Error executing task.", e1);
            }
        } else {
            if (t != null) {
                Throwable e1 = (t.getCause() == null) ? t : t.getCause();
                e1.printStackTrace();                
                logger.log(Level.SEVERE, "Error executing task.", e1);
            }
        }
    }
    
    private final static Logger logger = Logger.getLogger(SingleThreadDispatcher.class.getCanonicalName());
}
