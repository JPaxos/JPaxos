package lsr.common;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * A wrapper for a Runnable that kills the application if an unexpected
 * exception is thrown.
 * 
 * The {@link ScheduledThreadPoolExecutor} catches all exceptions thrown by the
 * tasks executed and saves them in the internal FutureTask implementation that
 * is used to wrap the task. When submitting a Future task, the exception is
 * accessible as a field of the task. Otherwise, if a Runnable is submitted,
 * there is no indication that an exception occurred, as the Runnable has no
 * place to store the exception. This behavior is undesirable, as it can hide
 * errors.
 * 
 * This class addresses this limitation, by wrapping the task that is to be
 * submitted with code that will exit the VM in case an error occurs. This is
 * intended for debugging only.
 * 
 * @author Nuno Santos (LSR)
 */
abstract public class Handler implements Runnable {

    /**
     * Calls <code>System.exit(1)</code> if the implementation in
     * {@link #handle()} throws an exception.
     */
    final public void run() {
        try {
            handle();
        } catch (Exception ex) {
            System.err.println("Unexpected exception. Aborting.");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * The task to be executed.
     */
    abstract public void handle();
}