package lsr.paxos.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import lsr.paxos.ReplicationException;
import lsr.paxos.client.Client;

public class EchoClient {
    private Client client;
    private final Random random = new Random();
    private final byte[] request;

    public EchoClient(int requestSize) {
        request = new byte[requestSize];
    }

    public void run() throws IOException, ReplicationException {
        client = new Client();
        client.connect();
/*
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line;

            line = reader.readLine();

            if (line == null) {
                break;
            }

            String[] args = line.split(" ");

            if (args[0].equals("bye")) {
                System.exit(0);
            }

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
        }*/
		
		int delay = 100;
		int maxRequests = 10;
		boolean isRandom = true;
		execute(delay, maxRequests, isRandom);
    }

    private void execute(int delay, int maxRequests, boolean isRandom) throws ReplicationException {
        long duration = 0;
        for (int i = 0; i < maxRequests; i++) {
            if (i != 0) {
                try {
                    Thread.sleep(isRandom ? random.nextInt(delay) : delay);
                } catch (InterruptedException e) {
                    break;
                }
            }
            // random.nextBytes(request);

            long start = System.currentTimeMillis();
            client.execute(request);
            duration += System.currentTimeMillis() - start;
        }

        System.err.println(String.format("Finished %d %4.2f\n", duration, (double) maxRequests /
                                                                          duration));
    }

    private static void showUsage() {
        System.out.println("EchoClient <RequestSize>");
    }

    private static void instructions() {
        System.out.println("Command: <delay> <maxRequests> <isRandom>");
    }

    public static void main(String[] args) throws IOException, ReplicationException {
        if (args.length == 0) {
            showUsage();
            System.exit(1);
        }
        instructions();
        EchoClient client = new EchoClient(Integer.parseInt(args[0]));
        client.run();
    }
}
