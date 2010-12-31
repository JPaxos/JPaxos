package lsr.common;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link DispatcherImpl} based on
 * {@link PriorityBlockingQueue} and {@link Thread}. All dispatched tasks are
 * executed sequentially in order of decreasing priority. If two tasks have the
 * same priority, task added first will be executed first. This dispatcher also
 * allows to schedule tasks to run after some delay or at fixed rate.
 * 
 * To start dispatcher call <code>start()</code> method. To stop the dispatcher
 * call <code>interrupt()</code> method.
 */
public class DispatcherImpl extends Thread implements Dispatcher {

    private int busyThreshold;

    /** Tasks waiting for immediate execution */
    private final PriorityBlockingQueue<InnerPriorityTask> taskQueue =
            new PriorityBlockingQueue<InnerPriorityTask>(4096);

    /**
     * Tasks scheduled for delayed execution. Once their delay expires, they are
     * put on the <code>taskQueue</code> where they'll be executed as soon as
     * the system has time for them
     */
    private final ScheduledThreadPoolExecutor scheduledTasks = new ScheduledThreadPoolExecutor(1);

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
    private final class InnerPriorityTask implements Comparable<PriorityTask>, PriorityTask {

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
        public InnerPriorityTask(Runnable task, Priority priority) {
            this.task = task;
            this.priority = priority;
        }

        public void cancel() {
            canceled = true;
        }

        public Priority getPriority() {
            return priority;
        }

        public long getDelay() {
            if (future == null) {
                return 0;
            } else {
                return future.getDelay(TimeUnit.MILLISECONDS);
            }
        }

        public long getSeqNum() {
            return seqNum;
        }

        public boolean isCanceled() {
            return canceled;
        }

        public int compareTo(PriorityTask o) {
            int res = this.priority.compareTo(o.getPriority());
            if (res == 0) {
                res = seqNum < o.getSeqNum() ? -1 : 1;
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
        private final InnerPriorityTask pTask;

        /**
         * Creates new instance of <code>TransferTask</code> class.
         * 
         * @param pTask
         */
        public TransferTask(InnerPriorityTask pTask) {
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
    public DispatcherImpl(String name) {
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
                DispatcherImpl.this.interrupt();
            }
        }));
    }

    public PriorityTask dispatch(Runnable task) {
        return dispatch(task, Priority.Normal);
    }

    public PriorityTask dispatch(Runnable task, Priority priority) {
        InnerPriorityTask pTask = new InnerPriorityTask(task, priority);
        taskQueue.add(pTask);
        return pTask;
    }

    public PriorityTask schedule(Runnable task, Priority priority, long delay) {
        InnerPriorityTask pTask = new InnerPriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.schedule(new TransferTask(pTask), delay,
                TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    public PriorityTask scheduleAtFixedRate(Runnable task, Priority priority, long initialDelay,
                                            long period) {
        InnerPriorityTask pTask = new InnerPriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.scheduleAtFixedRate(new TransferTask(pTask),
                initialDelay, period, TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    public PriorityTask scheduleWithFixedDelay(Runnable task, Priority priority, long initialDelay,
                                               long delay) {
        InnerPriorityTask pTask = new InnerPriorityTask(task, priority);
        ScheduledFuture<?> future = scheduledTasks.scheduleWithFixedDelay(new TransferTask(pTask),
                initialDelay, delay, TimeUnit.MILLISECONDS);
        pTask.future = future;
        return pTask;
    }

    public boolean amIInDispatcher() {
        if (Thread.currentThread() != this) {
            throw new AssertionError("Thread: " + Thread.currentThread().getName());
        }
        return true;
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                InnerPriorityTask pTask = taskQueue.take();
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
            logger.severe("Interrupted. Thread exiting.");
            // Do not exit, let shutdown hooks complete.
        } catch (Throwable e) {
            logger.log(Level.SEVERE, "Exception caught. Task canceled.", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public String toString() {
        int low = 0;
        int normal = 0;
        int high = 0;
        for (PriorityTask p : taskQueue) {
            switch (p.getPriority()) {
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

    private final static Logger logger = Logger.getLogger(DispatcherImpl.class.getCanonicalName());

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
