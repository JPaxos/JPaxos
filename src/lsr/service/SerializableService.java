package lsr.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import lsr.paxos.replica.Replica;

/**
 * This class provides skeletal implementation of {@link Service} interface to
 * simplify creating services. To create new service using this class programmer
 * needs to implement following methods:
 * <ul>
 * <li><code>execute</code></li>
 * <li><code>makeObjectSnapshot</code></li>
 * <li><code>updateToSnapshot</code></li>
 * </ul>
 * <p>
 * In most cases this methods will provide enough functionality. Creating
 * snapshots is invoked by framework. If more control for making snapshot is
 * needed then <code>Service</code> interface should be implemented.
 * <p>
 * All methods are called from the same thread, so it is not necessary to
 * synchronize them.
 * <p>
 * <b>Note:</b> The clients should use <code>SerializableClient</code> to
 * connect to services extending this class.
 * 
 */
public abstract class SerializableService extends SimplifiedService {
    /**
     * Executes one command from client on this state machine. This method will
     * be called by {@link Replica}.
     * 
     * @param value - command from client to execute on this service
     * @return generated reply which will be sent to client
     */
    protected abstract Object execute(Object value);

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
