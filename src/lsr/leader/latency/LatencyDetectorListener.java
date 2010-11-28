package lsr.leader.latency;

/**
 * Interface that should be implemented by classes interested in receiving
 * notifications of changes on the latency vector.
 * 
 * @author Benjamin Donz?
 */
public interface LatencyDetectorListener {

    /**
     * Called when the RTTVector changes.
     * 
     * @param rttVector The vector of RTT's estimations (nanosecond precision).
     */
    public void onNewRTTVector(double[] rttVector);
}
