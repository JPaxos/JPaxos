package lsr.common;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple implementation of {@link Dispatcher} based on
 * {@link LinkedBlockingDeque} and {@link Thread}. This run the new thread,
 * which waits for new events. When new event is added then it is handled. Every
 * event is executed in thread called <code>Dispatcher</code>
 */
public class Dispatcher extends Thread {

    private int busyThreshold;
    /** Tasks waiting for immediate execution */
    private final PriorityBlockingQueue<PriorityTask> taskQueue = new PriorityBlockingQueue<PriorityTask>(
            4096);

    /**
     * Tasks scheduled for delayed execution. Once their delay expires, they are
     * put on the _taskQueue where they'll be executed as soon as the system has
     * 
     * 
     * time for them
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
        public final Runnable task;
        public final Priority priority;
        private boolean canceled = false;
        private ScheduledFuture<?> future;
        /* Secondary class to implement FIFO order within the same class */
        private final long seqNum = seq.getAndIncrement();

        public PriorityTask(Runnable task, Priority priority) {
            this.task = task;
            this.priority = priority;
        }

        public void cancel() {
            this.canceled = true;
        }

        public long getDelay() {
            if (future == null) {
                return 0;
            } else {
                return future.getDelay(TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public int compareTo(PriorityTask o) {
            int res = this.priority.compareTo(o.priority);
            if (res == 0) {
                res = seqNum < o.seqNum ? -1 : 1;
            }
            return res;
        }

        @Override
        public String toString() {
            return "Task: " + task + ", Priority: " + priority;
        }

        public boolean isCanceled() {
            return canceled;
        }
    }

    /**
     * Transfers a task scheduled for execution into the main execution queue,
     * so that it is executed by the dispatcher thread and not by the internal
     * thread on the scheduled executor.
     */
    final class TransferTask implements Runnable {
        public final PriorityTask pTask;

        public TransferTask(PriorityTask pTask) {
            this.pTask = pTask;
        }

        /*
         * Executed on the ScheduledThreadPool thread. Must be careful to
         * synchronize with dispatcher thread.
         */
        @Override
        public void run() {
            taskQueue.add(pTask);
        }
    }

    /**
     * Initializes new instance of <code>Dispatcher</code>. This constructor
     * creates and starts new thread.
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
            @Override
            public void run() {
                Dispatcher.this.interrupt();
            }
        }));
    }

    // /**
    // * Initializes new instance of <code>Dispatcher</code>. This constructor
    // * creates and starts new thread.
    // */
    // public Dispatcher(ThreadGroup group, String name) {
    // super(group, name);
    // setDefaultUncaughtExceptionHandler(new KillOnExceptionHandler());
    //
    // }

    public PriorityTask dispatch(Runnable task) {
        return dispatch(task, Priority.Normal);
    }

    public PriorityTask dispatch(Runnable task, Priority priority) {
        PriorityTask pTask = new PriorityTask(task, priority);
        taskQueue.add(pTask);
        return pTask;
    }

    public PriorityTask schedule(Runnable task, Priority priority, long delay) {
        PriorityTask pTask = new PriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.schedule(new TransferTask(pTask), delay,
                TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    public PriorityTask scheduleAtFixedRate(Runnable task, Priority priority, long initialDelay,
                                            long period) {
        PriorityTask pTask = new PriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.scheduleAtFixedRate(new TransferTask(pTask),
                initialDelay, period, TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    public PriorityTask scheduleWithFixedDelay(Runnable task, Priority priority, long initialDelay,
                                               long delay) {
        PriorityTask pTask = new PriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.scheduleWithFixedDelay(new TransferTask(pTask),
                initialDelay, delay, TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    // public void executeAndWait(Runnable task) {
    // if (amIInDispatcher()) {
    // task.run();
    // } else {
    // Future<?> future = submit(task);
    // // Wait until the task is executed
    // try {
    // future.get();
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }
    // }

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

    @Override
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
