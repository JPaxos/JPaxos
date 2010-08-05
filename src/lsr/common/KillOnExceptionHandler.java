package lsr.common;

import java.lang.Thread.UncaughtExceptionHandler;

/**
 * Kills the system if an unexpected exception occurs in a daemon thread.
 * @author Nuno Santos (LSR)
 *
 */
public class KillOnExceptionHandler implements UncaughtExceptionHandler {
	public void uncaughtException(Thread t, Throwable e) {
		e.printStackTrace();
		System.exit(1);
	}
}
