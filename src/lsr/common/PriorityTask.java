package lsr.common;


public interface PriorityTask {

    /**
     * Attempts to cancel execution of this task. This attempt will fail if the
     * task has already completed, has already been canceled, or could not be
     * canceled for some other reason. If successful, and this task has not
     * started when cancel is called, this task should never run.
     * 
     * Subsequent calls to isCancelled() will always return true if this method
     * was called.
     */
    void cancel();

//    /**
//     * Returns the priority associated with this task.
//     * 
//     * @return the priority
//     */
//    Priority getPriority();

    /**
     * Returns the remaining delay associated with this task, in milliseconds.
     * 
     * @return the remaining delay; zero or negative values indicate that the
     *         delay has already elapsed
     */
    long getDelay();

    /**
     * Returns true if the <code>cancel()</code> method was called.
     * 
     * @return true if the <code>cancel()</code> method was called
     */
    boolean isCanceled();

//    public long getSeqNum();
}