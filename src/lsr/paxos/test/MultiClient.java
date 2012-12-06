package lsr.paxos.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import lsr.paxos.client.Client;
import lsr.paxos.client.ReplicationException;

public class MultiClient {
    private Vector<ClientThread> clients = new Vector<ClientThread>();

    // private RandomRequestGenerator requestGenerator;
    private final byte[] request;

    private AtomicInteger runningClients = new AtomicInteger(0);
    private final Semaphore finishedLock = new Semaphore(1);
    private long startTime;
    private int lastRequestCount;

    class ClientThread extends Thread {
        final Client client;
        private ArrayBlockingQueue<Integer> sends;

        public ClientThread() throws IOException {
            setDaemon(true);
            client = new Client();
            sends = new ArrayBlockingQueue<Integer>(128);
        }

        @Override
        public void run() {
            try {
                client.connect();

                while (!Thread.interrupted()) {
                    Integer count;
                    count = sends.take();
                    for (int i = 0; i < count; i++) {
                        // byte[] request = requestGenerator.generate();
                        if (Thread.interrupted()) {
                            return;
                        }
                        @SuppressWarnings("unused")
                        byte[] response;
                        response = client.execute(request);
                    }
                    int stillActive = runningClients.decrementAndGet();
                    if (stillActive == 0) {
                        finishedSend();
                    }
                }
            } catch (ReplicationException e) {
                System.err.println(e.getLocalizedMessage());
                System.exit(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void execute(int count) throws InterruptedException {
            sends.put(count);
        }

    }

    public MultiClient(int requestSize) {
        request = new byte[requestSize];
    }

    public void run() throws IOException, ReplicationException, InterruptedException {

        // requestGenerator = new RandomRequestGenerator();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }

            String[] args = line.split(" ");

            if (args[0].equals("bye")) {
                break;
            }

            if (args[0].equals("kill")) {
                for (ClientThread client : clients) {
                    client.interrupt();
                }
                System.exit(1);
                break;
            }

            if (args.length != 3) {
                System.err.println("Wrong command length! Expected:");
                printUsage();
                continue;
            }

            int clientCount;
            int requests;
            String isRandom;

            try {
                clientCount = Integer.parseInt(args[0]);
                requests = Integer.parseInt(args[1]);
                isRandom = args[2];
            } catch (NumberFormatException e) {
                System.err.println("Wrong argument! Expected:");
                printUsage();
                continue;
            }

            execute(clientCount, requests, isRandom);
        }
    }

    public void finishedSend() {

        long duration = System.currentTimeMillis() - startTime;
        System.err.println(String.format("Finished %d %4.2f\n", duration,
                (double) lastRequestCount / duration));

        finishedLock.release();
    }

    private void execute(int clientCount, int requests, String isRandom)
            throws ReplicationException, IOException, InterruptedException {

        finishedLock.acquire();

        for (int i = clients.size(); i < clientCount; i++) {
            ClientThread client = new ClientThread();
            client.start();
            clients.add(client);
        }

        runningClients.addAndGet(clientCount);

        startTime = System.currentTimeMillis();
        lastRequestCount = clientCount * requests;

        for (int i = 0; i < clientCount; i++) {
            clients.get(i).execute(requests);
        }
    }

    public static void main(String[] args) throws IOException, ReplicationException,
            InterruptedException {
        if (args.length == 0) {
            showUsage();
            System.exit(1);
        }
        printUsage();
        MultiClient client = new MultiClient(Integer.parseInt(args[0]));
        client.run();
    }

    private static void showUsage() {
        System.out.println("MultiClient <RequestSize>");
    }

    private static void printUsage() {
        System.out.println("bye");
        System.out.println("kill");
        System.out.println("<clientCount> <requestsPerClient> <any string>");
    }
}
