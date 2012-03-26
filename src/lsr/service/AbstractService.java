package lsr.service;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class which can be used to simplify creating new services. It adds
 * implementation for handling snapshot listeners.
 */
public abstract class AbstractService implements Service {
    /** Listeners which will be notified about new snapshot made by service */

    /**
     * Informs the service that the recovery process has been finished, i.e.
     * that the service is at least at the state later than by crashing.
     * 
     * Please notice, for some crash-recovery approaches this can mean that the
     * service is a lot further than by crash.
     * 
     * For many applications this has no real meaning.
     */
    public void recoveryFinished() {
    }

}