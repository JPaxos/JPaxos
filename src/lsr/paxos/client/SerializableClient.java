package lsr.paxos.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import lsr.common.Configuration;
import lsr.common.PID;
import lsr.paxos.ReplicationException;
import lsr.service.SerializableService;

/**
 * Class represents TCP connection to replica. It should be used by clients, to
 * communicates with service on replicas. Only one request can be sent by client
 * at the same time. After receiving reply, next request can be sent.
 * <p>
 * <b>Note</b>: To write object to underlying TCP socket java serialization is
 * used. Because of that this class should be used mainly for connecting to
 * services which extends <code>SerializableService</code>.
 * <p>
 * 
 * <p>
 * Example of usage:
 * <p>
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws IOException {
 *  Client client = new Client();
 *  client.connect();
 *  Object request = new String(&quot;my request&quot;);
 *  Object reply = client.execute(request);
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * @see SerializableService
 */
public class SerializableClient extends Client {

    /**
     * Loads the configuration from the default configuration file, as defined
     * in the class {@link Configuration}
     * 
     * @throws IOException if I/O error occurs while reading configuration
     */
    public SerializableClient() throws IOException {
        super();
    }

    /**
     * Creates new connection used by client to connect to replicas.
     * 
     * @param replicas - information about replicas to connect to
     */
    public SerializableClient(List<PID> replicas) {
        super(replicas);
    }

    /**
     * Sends request to replica, to execute service with specified object as
     * argument. This object should be known to replica, which generate reply.
     * This method will block until response from replica is received.
     * <p>
     * Note: Default java serialization will be used to send this object as byte
     * array.
     * 
     * @param request - argument for service
     * @return reply from service
     * @throws ReplicationException
     */
    public Object execute(Serializable request) throws IOException, ClassNotFoundException,
            ReplicationException {
        // serialize object to byte array
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        new ObjectOutputStream(byteOutputStream).writeObject(request);

        // execute command
        byte[] response = execute(byteOutputStream.toByteArray());

        // deserialize response
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(
                response));

        return objectInputStream.readObject();
    }
}
