package lsr.paxos.test;

import java.io.IOException;
import java.net.UnknownHostException;

import lsr.paxos.ReplicationException;
import lsr.paxos.client.Client;

public class PerfClient {
	/**
	 * @param args
	 * @throws IOException
	 * @throws ReplicationException
	 * @throws UnknownHostException
	 */
	public static void main(String[] args) throws IOException,
			ReplicationException {
		if (args.length != 1) {
			showUsage();
			System.exit(1);
		}
		Client client = new Client();
		client.connect();

		RandomRequestGenerator requestGenerator = new RandomRequestGenerator();

		int maxRequests = Integer.parseInt(args[0]);
		long start = System.currentTimeMillis();
		for (int i = 0; i < maxRequests; i++) {
			byte[] request = requestGenerator.generate();
			byte[] reply = client.execute(request);
			System.out.println(reply);
		}
		double duration = (System.currentTimeMillis() - start) / 1000.0;
		System.out.println(String.format("Time: %2.2f, Rate: %4.2f\n",
				duration, maxRequests / duration));
	}

	private static void showUsage() {
		System.out.println("Invalid arguments. Usage:\n"
				+ "   java lsr.client.PerfClient <nrequests>");

	}
}
