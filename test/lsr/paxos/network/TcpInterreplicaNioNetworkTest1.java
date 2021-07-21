package lsr.paxos.network;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.junit.Test;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

public class TcpInterreplicaNioNetworkTest1 extends TcpInterreplicaNioNetworkCommon {

    @Test(timeout = 3000)
    public void sendingConnecting() throws Exception {
        TcpInterreplicaNioNetwork network = new TcpInterreplicaNioNetwork();

        network.start();

        final ArrayList<Message> msgs = makeMessages();

        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(pid1.getReplicaPort()));
        Socket socket = server.accept();
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        assertEquals(0, inputStream.readInt());

        Thread.sleep(1500);

        for (Message m : msgs) {
            network.send(m, 1);
        }

        do {
            Message expected = msgs.remove(0);
            int size = inputStream.readInt();
            Message received = MessageFactory.create(inputStream);
            assertArrayEquals(expected.toByteArray(), received.toByteArray());
            assertEquals(expected.byteSize(), size);
        } while (!msgs.isEmpty());

        inputStream.close();
        socket.close();
        server.close();
    }
}