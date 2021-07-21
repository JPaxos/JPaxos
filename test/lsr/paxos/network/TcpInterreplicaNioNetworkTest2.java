package lsr.paxos.network;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.BitSet;

import org.junit.Test;

import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class TcpInterreplicaNioNetworkTest2 extends TcpInterreplicaNioNetworkCommon {

    @Test(timeout = 3000)
    public void receivingConnecting() throws Exception {
        TcpInterreplicaNioNetwork network = new TcpInterreplicaNioNetwork();

        network.start();

        final ArrayList<Message> msgs = makeMessages();

        Network.addMessageListener(MessageType.ANY, new MessageHandler() {

            public void onMessageSent(Message message, BitSet destinations) {
            }

            public void onMessageReceived(Message received, int sender) {
                Message expected = msgs.remove(0);
                assertArrayEquals(expected.toByteArray(), received.toByteArray());
                assertEquals(sender, 1);
                if (msgs.isEmpty())
                    network.send(new Alive(0, 0), 1);
            }
        });
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(pid1.getReplicaPort()));
        Socket socket = server.accept();
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
        assertEquals(0, inputStream.readInt());

        for (Message m : msgs) {
            outputStream.writeInt(m.byteSize());
            outputStream.write(m.toByteArray());
        }

        inputStream.read();

        assertEquals(0, msgs.size());

        inputStream.close();
        socket.close();
        server.close();
    }

}