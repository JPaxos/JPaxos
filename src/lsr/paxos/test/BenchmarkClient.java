package lsr.paxos.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import lsr.paxos.client.Client;

public class BenchmarkClient {
	private Client _client;
	private RandomRequestGenerator _requestGenerator;
	private final Random _random = new Random();

	public void run() throws IOException {
		_client = new Client();
		_client.connect();

		_requestGenerator = new RandomRequestGenerator();

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;

			String[] args = line.split(" ");

			if (args[0].equals("bye"))
				break;

			if (args.length != 3) {
				System.err.println("Wrong command length! Expected:");
				printUsage();
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
				printUsage();
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

			byte[] request = _requestGenerator.generate();

//			long start = System.currentTimeMillis();
			_client.execute(request);
//			duration += System.currentTimeMillis() - start;
		}

		System.err.println(String.format("Finished %d %4.2f\n", duration, (double) maxRequests / duration));
	}

	public static void main(String[] args) throws IOException {
		printUsage();
		BenchmarkClient client = new BenchmarkClient();
		client.run();
	}

	private static void printUsage() {
		System.out.println("delay maxRequests isRandom");
	}
}
