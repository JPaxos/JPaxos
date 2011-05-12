package lsr.paxos.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import lsr.paxos.ReplicationException;
import lsr.paxos.client.Client;

public class MapClient {
    private Client client;

    public void run() throws IOException, ReplicationException {
        client = new Client();
        client.connect();

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

            if (args.length != 2) {
                instructions();
                continue;
            }

            Long key = Long.parseLong(args[0]);
            Long value = Long.parseLong(args[1]);

            MapServiceCommand command = new MapServiceCommand(key, value);
            byte[] response = client.execute(command.toByteArray());
            ByteBuffer buffer = ByteBuffer.wrap(response);
            Long previousValue = buffer.getLong();
            System.out.println(String.format("Previous value for %d was %d", key, previousValue));
        }
    }

    private static void instructions() {
        System.out.println("Provide key-value pair of integers to insert to hash map");
        System.out.println("<key> <value>");
    }

    public static void main(String[] args) throws IOException, ReplicationException {
        instructions();
        MapClient client = new MapClient();
        client.run();
    }
}
