package lsr.paxos.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import lsr.common.FileConfigurationLoader;
import lsr.common.PID;
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
 * 	Client client = new Client();
 * 	client.connect();
 * 	Object request = new String(&quot;my request&quot;);
 * 	Object reply = client.execute(request);
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * @see SerializableService
 */
public class SerializableClient extends Client {

	/**
	 * Creates new client using default configuration file
	 * {@link FileConfigurationLoader#DEFAULT_CONFIG}.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs while reading configuration
	 * @see FileConfigurationLoader
	 */
	public SerializableClient() throws IOException {
		super();
	}

	/**
	 * Creates new client using specified loader to get configuration.
	 * 
	 * @param loader
	 *            - used to load configuration
	 * @throws IOException
	 *             if an I/O error occurs while loading configuration
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
	 * @param obj
	 *            - argument for service
	 * @return reply from service
	 */
	public Object execute(Serializable object) throws IOException, ClassNotFoundException {
		// serialize object to byte array
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		new ObjectOutputStream(byteOutputStream).writeObject(object);

		// execute command
		byte[] response = execute(byteOutputStream.toByteArray());

		// deserialize response
		ObjectInputStream objectInputStream = new ObjectInputStream(
				new ByteArrayInputStream(response));

		return objectInputStream.readObject();
	}
}
