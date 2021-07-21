package lsr.paxos.network;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.BitSet;

import org.junit.Test;

import lsr.paxos.messages.Alive;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class TcpInterreplicaNioNetworkTest4 extends TcpInterreplicaNioNetworkCommon {

    @Test(timeout = 3000)
    public void receivingAccepting() throws Exception {
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
        Socket socket = new Socket("localhost", pid0.getReplicaPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
        outputStream.writeInt(1);

        for (Message m : msgs) {
            outputStream.writeInt(m.byteSize());
            outputStream.write(m.toByteArray());
        }

        inputStream.read();

        assertEquals(0, msgs.size());

        inputStream.close();
        socket.close();
    }

}