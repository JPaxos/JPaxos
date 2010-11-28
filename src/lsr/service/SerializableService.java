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

    /**
     * Updates the current state of <code>Service</code> to state from snapshot.
     * This method will be called after recovery to restore previous state, or
     * if we received new one from other replica (using catch-up).
     * <p>
     * Snapshot argument is deserialized version of object created in
     * {@link #makeObjectSnapshot()}
     * 
     * @param snapshot - data used to update to new state
     */
    protected abstract void updateToSnapshot(Object snapshot);

    /**
     * Makes snapshot for current state of <code>Service</code>.
     * <p>
     * The same data created in this method, will be used to update state from
     * other snapshot using {@link #updateToSnapshot(Object)} method.
     * 
     * @return the data containing current state
     */
    protected abstract Object makeObjectSnapshot();

    protected final byte[] execute(byte[] value) {
        try {
            return byteArrayFromObject(execute(byteArrayToObject(value)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected final byte[] makeSnapshot() {
        try {
            return byteArrayFromObject(makeObjectSnapshot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected final void updateToSnapshot(byte[] snapshot) {
        try {
            updateToSnapshot(byteArrayToObject(snapshot));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
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
