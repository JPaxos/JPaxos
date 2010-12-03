package lsr.common;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link Dispatcher} based on {@link PriorityBlockingQueue}
 * and {@link Thread}. All dispatched tasks are executed sequentially in order
 * of decreasing priority. If two tasks have the same priority, task added first
 * will be executed first. This dispatcher also allows to schedule tasks to run
 * after some delay or at fixed rate.
 * 
 * To start dispatcher call <code>start()</code> method. To stop the dispatcher
 * call <code>interrupt()</code> method.
 */
public class Dispatcher extends Thread {

    private int busyThreshold;

    /** Tasks waiting for immediate execution */
    private final PriorityBlockingQueue<PriorityTask> taskQueue =
            new PriorityBlockingQueue<PriorityTask>(4096);

    /**
     * Tasks scheduled for delayed execution. Once their delay expires, they are
     * put on the <code>taskQueue</code> where they'll be executed as soon as
     * the system has time for them
     */
    private final ScheduledThreadPoolExecutor scheduledTasks = new ScheduledThreadPoolExecutor(1);

    /**
     * Priorities are strict, in the sense that a task from a lower priority is
     * executed only when there are no more tasks from higher priorities.
     */
    public enum Priority {
        High, Normal, Low
    }

    /**
     * Implements FIFO within priority classes.
     */
    private final static AtomicLong seq = new AtomicLong();

    /**
     * PriorityTask is a {@link Runnable} wrapped around with:
     * <ul>
     * <li>unique sequence number
     * <li>priority
     * </ul>
     * A PriorityTask can be set as canceled.
     * 
     * @author (LSR)
     */
    public final class PriorityTask implements Comparable<PriorityTask> {

        private final Runnable task;
        private final Priority priority;
        private boolean canceled = false;
        private ScheduledFuture<?> future;
        /* Secondary class to implement FIFO order within the same class */
        private final long seqNum = seq.getAndIncrement();

        /**
         * Create new instance of <code>Priority</code> class.
         * 
         * @param task - the underlying task
         * @param priority - the priority associated with this task
         */
        public PriorityTask(Runnable task, Priority priority) {
            this.task = task;
            this.priority = priority;
        }

        /**
         * Attempts to cancel execution of this task. This attempt will fail if
         * the task has already completed, has already been canceled, or could
         * not be canceled for some other reason. If successful, and this task
         * has not started when cancel is called, this task should never run.
         * 
         * Subsequent calls to isCancelled() will always return true if this
         * method was called.
         */
        public void cancel() {
            canceled = true;
        }

        /**
         * Returns the priority associated with this task.
         * 
         * @return the priority
         */
        public Priority getPriority() {
            return priority;
        }

        /**
         * Returns the remaining delay associated with this task, in
         * milliseconds.
         * 
         * @return the remaining delay; zero or negative values indicate that
         *         the delay has already elapsed
         */
        public long getDelay() {
            if (future == null) {
                return 0;
            } else {
                return future.getDelay(TimeUnit.MILLISECONDS);
            }
        }

        /**
         * Returns true if the <code>cancel()</code> method was called.
         * 
         * @return true if the <code>cancel()</code> method was called
         */
        public boolean isCanceled() {
            return canceled;
        }

        public int compareTo(PriorityTask o) {
            int res = this.priority.compareTo(o.priority);
            if (res == 0) {
                res = seqNum < o.seqNum ? -1 : 1;
            }
            return res;
        }

        public String toString() {
            return "Task: " + task + ", Priority: " + priority;
        }
    }

    /**
     * Transfers a task scheduled for execution into the main execution queue,
     * so that it is executed by the dispatcher thread and not by the internal
     * thread on the scheduled executor.
     */
    private final class TransferTask implements Runnable {
        private final PriorityTask pTask;

        /**
         * Creates new instance of <code>TransferTask</code> class.
         * 
         * @param pTask
         */
        public TransferTask(PriorityTask pTask) {
            this.pTask = pTask;
        }

        /**
         * Executed on the ScheduledThreadPool thread. Must be careful to
         * synchronize with dispatcher thread.
         */
        public void run() {
            taskQueue.add(pTask);
        }
    }

    /**
     * Initializes new instance of <code>Dispatcher</code> class.
     */
    public Dispatcher(String name) {
        super(name);
        /*
         * When the JVM is killed with an interrupt, the shutdown hooks are
         * executed while the application threads continue running. This can
         * cause problems if the shutdown hooks clear some state that is
         * required by the application threads. For instance, loggers are reset,
         * so access to loggers may fail unexpectedly. This shutdown hook kills
         * the dispatcher thread earlier on the shutdown process, but it doesn't
         * prevent race conditions caused by other shutdown hooks.
         */
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                Dispatcher.this.interrupt();
            }
        }));
    }

    /**
     * Create and executes one-shot action with normal priority.If there is more
     * than one task enabled in given moment, tasks are executed sequentially in
     * order of priority.
     * 
     * @param task - the task to execute
     * 
     * @return a PriorityTask representing pending completion of the task.
     */
    public PriorityTask dispatch(Runnable task) {
        return dispatch(task, Priority.Normal);
    }

    /**
     * Creates and executes one-shot action. If there is more than one task
     * enabled in given moment, tasks are executed sequentially in order of
     * priority.
     * 
     * @param task - the task to execute
     * @param priority - the priority of the task
     * 
     * @return a PriorityTask representing pending completion of the task
     */
    public PriorityTask dispatch(Runnable task, Priority priority) {
        PriorityTask pTask = new PriorityTask(task, priority);
        taskQueue.add(pTask);
        return pTask;
    }

    /**
     * Creates and executes one-shot action that becomes enabled after the given
     * delay. If there is more than one task enabled in given moment, tasks are
     * executed sequentially in order of priority.
     * 
     * @param task - the task to execute
     * @param priority - the priority of the task
     * @param delay - the time in milliseconds from now to delay execution
     * 
     * @return a PriorityTask representing pending completion of the task
     */
    public PriorityTask schedule(Runnable task, Priority priority, long delay) {
        PriorityTask pTask = new PriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.schedule(new TransferTask(pTask), delay,
                TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given period; that is
     * executions will commence after <code>initialDelay</code> then
     * <code>initialDelay+period</code>, then
     * <code>initialDelay + 2 * period</code>, and so on. If any execution of
     * the task encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or termination
     * of the dispatcher. If any execution of this task takes longer than its
     * period, then subsequent executions may start late, but will not
     * concurrently execute.
     * 
     * @param task - the task to execute
     * @param priority - the priority of the task
     * @param initialDelay - the time in milliseconds to delay first execution
     * @param period - the period in milliseconds between successive executions
     * 
     * @return a PriorityTask representing pending completion of the task
     */
    public PriorityTask scheduleAtFixedRate(Runnable task, Priority priority, long initialDelay,
                                            long period) {
        PriorityTask pTask = new PriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.scheduleAtFixedRate(new TransferTask(pTask),
                initialDelay, period, TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given delay between
     * the termination of one execution and the commencement of the next. If any
     * execution of the task encounters an exception, subsequent executions are
     * suppressed. Otherwise, the task will only terminate via cancellation or
     * termination of the dispatcher.
     * 
     * @param task - the task to execute
     * @param priority - the priority of the task
     * @param initialDelay - the time in milliseconds to delay first execution
     * @param delay - the period in millisecond between successive executions
     * 
     * @return a PriorityTask representing pending completion of the task
     */
    public PriorityTask scheduleWithFixedDelay(Runnable task, Priority priority, long initialDelay,
                                               long delay) {
        PriorityTask pTask = new PriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.scheduleWithFixedDelay(new TransferTask(pTask),
                initialDelay, delay, TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    /**
     * Checks whether current thread is the same as the thread associated with
     * this dispatcher.
     * 
     * @return true if the current and dispatcher threads are equals, false
     *         otherwise
     */
    public boolean amIInDispatcher() {
        if (Thread.currentThread() != this) {
            throw new AssertionError("Thread: " + Thread.currentThread().getName());
        }
        return true;
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                PriorityTask pTask = taskQueue.take();
                if (pTask.isCanceled()) {
                    // If this task is also scheduled as a future,
                    // stop future executions
                    if (pTask.future != null) {
                        pTask.future.cancel(false);
                        pTask.future = null;
                    }
                } else {
                    pTask.task.run();
                }
            }
        } catch (InterruptedException e) {
            // If this is called during a shutdown, there's no
            // guarantee that the logger is still working.
            // but we try to log it anyway.
            _logger.severe("Interrupted. Thread exiting.");
            // Do not exit, let shutdown hooks complete.
        } catch (Throwable e) {
            _logger.log(Level.SEVERE, "Exception caught. Task canceled.", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public String toString() {
        int low = 0;
        int normal = 0;
        int high = 0;
        for (PriorityTask p : taskQueue) {
            switch (p.priority) {
                case High:
                    high++;
                    break;
                case Normal:
                    normal++;
                    break;
                case Low:
                    low++;
                    break;
            }
        }
        return "High:" + high + ",Normal:" + normal + ",Low:" + low;
    }

    private final static Logger _logger = Logger.getLogger(Dispatcher.class.getCanonicalName());

    /*
     * TODO: [NS] Queue size grows to very high numbers with lots of empty
     * tasks. Find a better way of managing overload.
     */
    public int getQueueSize() {
        return taskQueue.size();
    }

    public boolean isBusy() {
        return taskQueue.size() >= busyThreshold;
    }

    public int getBusyThreshold() {
        return busyThreshold;
    }

    public void setBusyThreshold(int busyThreshold) {
        this.busyThreshold = busyThreshold;
    }
}
