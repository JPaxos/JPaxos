package lsr.common;

public interface Dispatcher {

    /**
     * Create and executes one-shot action with normal priority.If there is more
     * than one task enabled in given moment, tasks are executed sequentially in
     * order of priority.
     * 
     * @param task - the task to execute
     * 
     * @return a PriorityTask representing pending completion of the task.
     */
    PriorityTask dispatch(Runnable task);

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
//    PriorityTask dispatch(Runnable task, Priority priority);

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
    PriorityTask schedule(Runnable task, long delay);

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
    PriorityTask scheduleAtFixedRate(Runnable task, long initialDelay, long period);

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
    PriorityTask scheduleWithFixedDelay(Runnable task, long initialDelay, long delay);

    /**
     * Checks whether current thread is the same as the thread associated with
     * this dispatcher.
     * 
     * @return true if the current and dispatcher threads are equals, false
     *         otherwise
     */
    boolean amIInDispatcher();

    void start();
}