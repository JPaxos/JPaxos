package lsr.paxos.recovery;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import lsr.common.Dispatcher;
import lsr.common.DispatcherImpl.Priority;
import lsr.common.PriorityTask;

public class MockDispatcher implements Dispatcher {
    private Queue<InnerPriorityTask> tasks = new LinkedBlockingQueue<InnerPriorityTask>();
    private boolean inDispatcher = false;

    public PriorityTask dispatch(Runnable task) {
        tasks.add(new InnerPriorityTask(task, Priority.Normal));
        return null;
    }

    public PriorityTask dispatch(Runnable task, Priority priority) {
        tasks.add(new InnerPriorityTask(task, priority));
        return null;
    }

    public PriorityTask schedule(Runnable task, Priority priority, long delay) {
        tasks.add(new InnerPriorityTask(task, priority));
        return null;
    }

    public PriorityTask scheduleAtFixedRate(Runnable task, Priority priority, long initialDelay,
                                            long period) {
        tasks.add(new InnerPriorityTask(task, priority));
        return null;
    }

    public PriorityTask scheduleWithFixedDelay(Runnable task, Priority priority, long initialDelay,
                                               long delay) {
        tasks.add(new InnerPriorityTask(task, priority));
        return null;
    }

    public boolean amIInDispatcher() {
        return inDispatcher;
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

    public void executeAll() {
        inDispatcher = true;
        while (!tasks.isEmpty()) {
            InnerPriorityTask priorityTask = tasks.poll();
            priorityTask.task.run();
        }
        inDispatcher = false;
    }

    private class InnerPriorityTask implements PriorityTask {
        private final Runnable task;
        private final Priority priority;
        private boolean canceled = false;

        public InnerPriorityTask(Runnable task, Priority priority) {
            this.task = task;
            this.priority = priority;
        }

        public void cancel() {
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
    }
}