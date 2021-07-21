package lsr.paxos.network;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import org.junit.Test;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

public class TcpInterreplicaNioNetworkTest3 extends TcpInterreplicaNioNetworkCommon {

    @Test(timeout = 3000)
    public void sendingAccepting() throws Exception {
        TcpInterreplicaNioNetwork network = new TcpInterreplicaNioNetwork();

        network.start();

        final ArrayList<Message> msgs = makeMessages();

        Socket socket = new Socket("localhost", pid0.getReplicaPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
        outputStream.writeInt(1);

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
    }
}