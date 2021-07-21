package lsr.common;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public final class NewSingleThreadDispatcher {
    private final Thread workerThread;
    private final Thread maintainerThread;
    private final BlockingDeque<Runnable> queue;

    public static class UniqueInstant implements Comparable<UniqueInstant> {
        public final Instant instant;

        public UniqueInstant(Instant instant) {
            this.instant = instant;
        }

        public int compareTo(UniqueInstant other) {
            if (equals(other))
                return 0;
            int ret = instant.compareTo(other.instant);
            if (ret != 0)
                return ret;
            if (hashCode() < instant.hashCode())
                return 1;
            return -1;
        }
    }

    private final NavigableMap<UniqueInstant, ScheduledTask> scheduledTasks = new TreeMap<UniqueInstant, NewSingleThreadDispatcher.ScheduledTask>();

    private static class ScheduledTask {
        private Instant time;
        private final Duration repeatPeriod;
        private final Runnable task;

        public ScheduledTask(Runnable task, Instant time, Duration repeatPeriod) {
            this.task = task;
            this.time = time;
            this.repeatPeriod = repeatPeriod;
        }
    }

    public NewSingleThreadDispatcher(String name) {
        queue = new LinkedBlockingDeque<Runnable>();
        workerThread = new Thread(this::run, name);
        maintainerThread = new Thread(this::maintain, name + "-maintainer");
        workerThread.setDaemon(true);
        maintainerThread.setDaemon(true);
        workerThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
        maintainerThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
    }

    public void start() {
        maintainerThread.start();
        workerThread.start();
    }

    public void submit(Runnable r) {
        queue.addLast(r);
    }

    public void submitFirst(Runnable r) {
        queue.addFirst(r);
    }

    public void schedule(Runnable r, Instant time) {
        schedule(r, time, Duration.ZERO);
    }

    public void schedule(Runnable r, Instant time, Duration repeatPeriod) {
        synchronized (scheduledTasks) {
            scheduledTasks.put(new UniqueInstant(time), new ScheduledTask(r, time, repeatPeriod));
            scheduledTasks.notify();
        }
    }

    public void executeAndWait(final Runnable r) {
        if (Thread.currentThread() == workerThread) {
            r.run();
            return;
        }

        Object lock = new Object();
        synchronized (lock) {
            queue.addLast(() -> {
                r.run();
                synchronized (lock) {
                    lock.notify();
                }
            });
            try {
                lock.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void executeFirstAndWait(final Runnable r) {
        if (Thread.currentThread() == workerThread) {
            r.run();
            return;
        }

        Object lock = new Object();
        synchronized (lock) {
            queue.addFirst(() -> {
                r.run();
                synchronized (lock) {
                    lock.notify();
                }
            });
            try {
                lock.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void run() {
        try {
            while (true)
                queue.takeFirst().run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void maintain() {
        try {
            synchronized (scheduledTasks) {
                while (true) {
                    while (scheduledTasks.isEmpty()) {
                        scheduledTasks.wait();
                    }
                    // see if the first one expired
                    Instant now = Instant.now();
                    Duration remainingWait = Duration.between(now,
                            scheduledTasks.firstKey().instant);
                    if (remainingWait.isNegative() || remainingWait.isZero()) {
                        // if yes - put task on the _beginning_ of the queue
                        ScheduledTask task = scheduledTasks.pollFirstEntry().getValue();
                        queue.addFirst(task.task);
                        // reschedule if needed
                        if (!task.repeatPeriod.isZero()) {
                            do {
                                task.time = task.time.plus(task.repeatPeriod);
                            } while (task.time.isBefore(now));
                            scheduledTasks.put(new UniqueInstant(task.time), task);
                        }
                    } else {
                        // if no - wait until it expires or something wakes us
                        scheduledTasks.wait(remainingWait.toMillis(),
                                remainingWait.toNanosPart() % 1_000_000);
                    }
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean amIInDispatcher() {
        return Thread.currentThread() == workerThread;
    }

    public void checkInDispatcher() {
        assert amIInDispatcher() : "Wrong thread: " + Thread.currentThread().getName();
    }

    public void remove(Runnable taskToRemove) {
        synchronized (scheduledTasks) {
            queue.remove(taskToRemove);
            List<UniqueInstant> keysToRemove = new LinkedList<UniqueInstant>();
            for (Entry<UniqueInstant, ScheduledTask> e : scheduledTasks.entrySet())
                if (e.getValue().task == taskToRemove)
                    keysToRemove.add(e.getKey());
            for (UniqueInstant key : keysToRemove)
                scheduledTasks.remove(key);
        }
    }

}
