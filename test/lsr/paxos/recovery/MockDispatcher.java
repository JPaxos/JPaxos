package lsr.paxos.recovery;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import lsr.common.Dispatcher;
import lsr.common.PriorityTask;

public class MockDispatcher implements Dispatcher {
    private Queue<InnerPriorityTask> activeTasks = new LinkedBlockingQueue<InnerPriorityTask>();
    private Queue<InnerPriorityTask> delayedTasks = new LinkedBlockingQueue<InnerPriorityTask>();
    private long currentTime = 0;
    private boolean inDispatcher = false;
    private boolean forceInDispatcher = false;

    public PriorityTask dispatch(Runnable task) {
        InnerPriorityTask priorityTask = new InnerPriorityTask(task, Priority.Normal);
        activeTasks.add(priorityTask);
        return priorityTask;
    }

    public PriorityTask dispatch(Runnable task, Priority priority) {
        InnerPriorityTask priorityTask = new InnerPriorityTask(task, priority);
        activeTasks.add(priorityTask);
        return priorityTask;
    }

    public PriorityTask schedule(Runnable task, Priority priority, long delay) {
        return scheduleAtFixedRate(task, priority, delay, 1000000000);
    }

    public PriorityTask scheduleAtFixedRate(Runnable task, Priority priority, long initialDelay,
                                            long period) {
        InnerPriorityTask priorityTask = new InnerPriorityTask(task, priority,
                initialDelay + currentTime, period);
        delayedTasks.add(priorityTask);
        if (initialDelay == 0) {
            activeTasks.add(priorityTask);
        }
        return priorityTask;
    }

    public PriorityTask scheduleWithFixedDelay(Runnable task, Priority priority, long initialDelay,
                                               long delay) {
        return scheduleAtFixedRate(task, priority, initialDelay, delay);
    }

    public boolean amIInDispatcher() {
        return inDispatcher || forceInDispatcher;
    }

    public void start() {
    }

    public int getQueueSize() {
        throw new RuntimeException("Not implemented.");
    }

    public boolean isBusy() {
        throw new RuntimeException("Not implemented.");
    }

    public int getBusyThreshold() {
        throw new RuntimeException("Not implemented.");
    }

    public void setBusyThreshold(int busyThreshold) {
        throw new RuntimeException("Not implemented.");
    }

    public void forceBeingInDispatcher() {
        forceInDispatcher = true;
    }

    public void execute() {
        for (InnerPriorityTask task : delayedTasks) {
            if (task.getNextExecution() <= currentTime) {
                activeTasks.add(task);
            }
        }

        inDispatcher = true;
        while (!activeTasks.isEmpty()) {
            InnerPriorityTask task = activeTasks.poll();
            if (task.isCanceled()) {
                continue;
            }

            task.run();
        }
        inDispatcher = false;
    }

    /**
     * Moves the current time specified number of milliseconds to the future.
     * 
     * @param milliseconds
     */
    public void advanceTime(long milliseconds) {
        currentTime += milliseconds;
    }

    private static class InnerPriorityTask implements PriorityTask {
        private final Runnable task;
        private final Priority priority;
        private boolean canceled = false;

        private long nextExecution = 0;
        private long period = 0;

        public InnerPriorityTask(Runnable task, Priority priority) {
            this.task = task;
            this.priority = priority;
        }

        public InnerPriorityTask(Runnable task, Priority priority, long initialDelay, long period) {
            this.task = task;
            this.priority = priority;
            this.nextExecution = initialDelay;
            this.period = period;
        }

        public void cancel() {
            canceled = true;
        }

        public void run() {
            task.run();
            nextExecution += period;
        }

        public Priority getPriority() {
            return priority;
        }

        public long getDelay() {
            return 0;
        }

        public boolean isCanceled() {
            return canceled;
        }

        public long getSeqNum() {
            return 0;
        }

        public long getNextExecution() {
            return nextExecution;
        }
    }
}