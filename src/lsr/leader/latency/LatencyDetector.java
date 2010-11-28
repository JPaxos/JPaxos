/**
 * 
 */
package lsr.leader.latency;

import java.util.concurrent.ExecutionException;

/**
 * Methods that all latency detector implementations should provide.
 * 
 * @author Donz? Benjamin
 */
public interface LatencyDetector {

    /**
     * Activates the latency detector.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws Exception
     */
    public void start() throws Exception;

    /**
     * Stop the activities of the LD
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void stop() throws Exception;

    /** Returns the current RTT vector. Nanosecond precision */
    public double[] getRTTVector();

    /** Registers a listener for latency detector events. */
    public void registerLatencyDetectorListener(LatencyDetectorListener listener);

    /** Removes a listener for latency detector events. */
    public void removeLatencyDetectorListener(LatencyDetectorListener listener);
}
