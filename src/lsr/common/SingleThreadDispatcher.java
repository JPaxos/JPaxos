package lsr.common;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
final public class SingleThreadDispatcher extends ScheduledThreadPoolExecutor {
	private final NamedThreadFactory ntf;

	private CopyOnWriteArrayList<DispatcherListener> listeners = new CopyOnWriteArrayList<DispatcherListener>();

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

		// // Debugging
		// this.scheduleAtFixedRate(new Runnable() {
		// @Override
		// public void run() {
		// BlockingQueue<Runnable> queue =
		// SingleThreadDispatcher.super.getQueue();
		// StringBuffer sb = new StringBuffer(512);
		// sb.append(ntf.name + " Work queue Size:" + queue.size());
		// int i = 0;
		// for (Runnable runnable : queue) {
		// if (i%8 == 0) {
		// sb.append(i + " " + runnable);
		// }
		// i++;
		// }
		// _logger.warning(sb.toString());
		// }}, 2000, 2000, TimeUnit.MILLISECONDS);
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
	 * directly, otherwise hands it over to the dispatcher thread.
	 * 
	 * @param handler
	 */
	public void executeDirect(Handler handler) {
		if (amIInDispatcher()) {
			handler.run();
		} else {
			execute(handler);
		}
	}

	// private void delay() {
	// // Slow down the thread that is scheduling
	// if (!Thread.currentThread().getName().equals("Dispatcher") &&
	// super.getQueue().size()>250)
	// {
	// try {
	// Thread.sleep(100);
	// } catch (InterruptedException e) {
	// // TO DO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// }

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

	public int getQueuedIncomingMsgs() {
		return queuedIncomingPriorityMsgs.get();
	}

	public void incomingMessageHandled() {
		int count = queuedIncomingPriorityMsgs.decrementAndGet();
		if (count == 0) {
			fireIncomingQueueEmpty();
		}
	}

	private void fireIncomingQueueEmpty() {
		for (DispatcherListener listener : listeners) {
			listener.onIncomingQueueEmpty();
		}
	}

	public void queueIncomingMessage(Runnable event) {
		queuedIncomingPriorityMsgs.incrementAndGet();
		execute(event);
	}

	public void registerDispatcherListener(DispatcherListener listener) {
		if (listeners.contains(listener)) {
			throw new AssertionError("Listener alrady registered");
		}
		this.listeners.add(listener);
	}

	public void unregisterDispatcherListener(DispatcherListener listener) {
		listeners.remove(listener);
	}

	/**
	 * Handles exceptions thrown by the executed tasks.
	 * Kills the process on exception as tasks shouldn't
	 * throw exceptions under normal conditions.
	 */
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		/* If the task is wrapped on a Future, any exception
		 * will be stored on the Future and t will be null
		 */
		if (r instanceof Future<?>) {
			Future<?> ft = (Future<?>)r;
			try {
				ft.get(0, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else {
			if (t != null) {
				t.printStackTrace();
				System.exit(-1);
			}
		}
	}

	@SuppressWarnings("unused")
	private final static Logger _logger = Logger.getLogger(SingleThreadDispatcher.class.getCanonicalName());

	private AtomicInteger queuedIncomingPriorityMsgs = new AtomicInteger(0);
}
