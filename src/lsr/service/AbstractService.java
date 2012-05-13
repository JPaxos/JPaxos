package lsr.service;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

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
	
	public byte[] takeSnapshot() throws IOException{
		byte[] snapshot = byteArrayFromObject(makeObjectSnapshot());
		return snapshot;
	}
	
	protected Object byteArrayToObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return ois.readObject();
    }
	
    protected byte[] byteArrayFromObject(Object object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        new ObjectOutputStream(bos).writeObject(object);
        return bos.toByteArray();
    }
}