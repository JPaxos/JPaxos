package lsr.paxos.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import lsr.paxos.client.Client;

public class EchoClient {
	private Client _client;
	private final Random _random = new Random();
	private final int _requestSize;
	private final byte[] _request;

	public EchoClient(int requestSize) {
		_requestSize = requestSize;
		_request = new byte[_requestSize];
	}

	public void run() throws IOException {
		_client = new Client();
		_client.connect();

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line;

			line = reader.readLine();

			if (line == null)
				break;

			String[] args = line.split(" ");

			if (args[0].equals("bye"))
				break;

			if (args.length != 3) {
				System.err.println("Wrong command length! Expected:");
				instructions();
				continue;
			}

			int delay;
			int maxRequests;
			boolean isRandom;

			try {
				delay = Integer.parseInt(args[0]);
				maxRequests = Integer.parseInt(args[1]);
				isRandom = Boolean.parseBoolean(args[2]);
			} catch (NumberFormatException e) {
				System.err.println("Wrong argument! Expected:");
				instructions();
				continue;
			}

			execute(delay, maxRequests, isRandom);
		}
	}

	private void execute(int delay, int maxRequests, boolean isRandom) {
		long duration = 0;
		for (int i = 0; i < maxRequests; i++) {

			if (i != 0) {
				try {
					Thread.sleep(isRandom ? _random.nextInt(delay) : delay);
				} catch (InterruptedException e) {
					break;
				}
			}
			_random.nextBytes(_request);

			long start = System.currentTimeMillis();
			_client.execute(_request);
			duration += System.currentTimeMillis() - start;
		}

		System.err.println(String.format("Finished %d %4.2f\n", duration, (double) maxRequests / duration));
	}


	private static void showUsage() {
		System.out.println("EchoClient <RequestSize>");
	}

	private static void instructions() {
		System.out.println("Command: <delay> <maxRequests> <isRandom>");
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			showUsage();
			System.exit(1);
		}
		instructions();
		EchoClient client = new EchoClient(Integer.parseInt(args[0]));
		client.run();
	}
}
