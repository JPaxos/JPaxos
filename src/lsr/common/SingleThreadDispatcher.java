package lsr.common;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    // private final CountDownLatch latch = new CountDownLatch(1);

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
        setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                logger.severe("Task rejected: " + r);
            }
        });
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

    // @Override
    // protected void beforeExecute(Thread t, Runnable r) {
    // super.beforeExecute(t, r);
    // try {
    // latch.await();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // // logger.info("");
    // }

    /**
     * Handles exceptions thrown by the executed tasks. Kills the process on
     * exception as tasks shouldn't throw exceptions under normal conditions.
     */
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        // logger.info("");
        /*
         * If the task is wrapped on a Future, any exception will be stored on
         * the Future and t will be null
         */
        if (t == null && r instanceof FutureTask<?>) {
            // if (r instanceof FutureTask<?>) {
            // FutureTasks may be scheduled for repeated execution. In that
            // case, the task
            // may be scheduled for further re-execution, and thus there is no
            // result to return.
            // The get() method would block in this case.
            FutureTask<?> fTask = (FutureTask<?>) r;
            // logger.info("Task: " + fTask + ", isDone: " + fTask.isDone() +
            // ", isCancelled: " + fTask.isCancelled());
            if (!fTask.isDone()) {
                // Since the task is still scheduled for execution, it did not
                // throw any exception.
                return;
            } else {
                // Either a repeated task that is done, or a single shot task.
                // Future<?> future = (Future<?>) r;
                // assert future.isDone() :
                // "Task is not finished, cannot call get() method on future.";
                // We don't care about the result, only on the exception.
                try {
                    fTask.get(0, TimeUnit.MILLISECONDS);
                } catch (CancellationException ce) {
                    logger.info("Task was cancelled: " + r);
                } catch (ExecutionException ee) {
                    t = ee.getCause();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // ignore/reset
                } catch (TimeoutException e) {
                    // The task should already be finished, so get() should
                    // return immediately.
                    // However, if the code executed by this task calls
                    // cancel(true) on the Runnable,
                    // the get() may block. Therefore, if it throws a
                    // TimeoutException, check
                    // the implementation of the task to see if it is calling
                    // cancel().
                    logger.log(Level.SEVERE, "Timeout retrieving exception object. Task: " + r, e);
                }
            }
        }
        if (t != null) {
            // It is a severe error, print it to the console as well as to the
            // log.
            if (printErrorsToConsole) {
                t.printStackTrace();
                printErrorsToConsole = false;
            }
            logger.log(Level.SEVERE, "Error executing task.", t);
        }
    }

    private boolean printErrorsToConsole = true;

    public void start() {
        // latch.countDown();
    }

    private final static Logger logger = Logger.getLogger(SingleThreadDispatcher.class.getCanonicalName());

}
