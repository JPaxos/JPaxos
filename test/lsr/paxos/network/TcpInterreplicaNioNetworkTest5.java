package lsr.paxos.network;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

import org.junit.Test;

import lsr.common.Reply;
import lsr.paxos.Snapshot;
import lsr.paxos.messages.Alive;
import lsr.paxos.messages.CatchUpSnapshot;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;

public class TcpInterreplicaNioNetworkTest5 extends TcpInterreplicaNioNetworkCommon {

    @Test(timeout = 30000)
    public void receivingAccepting() throws Exception {
        TcpInterreplicaNioNetwork network = new TcpInterreplicaNioNetwork();

        network.start();

        final ArrayList<Message> msgs = new ArrayList<>();
        Snapshot snapshot = new Snapshot();
        snapshot.setLastReplyForClient(new HashMap<Long, Reply>());
        snapshot.setNextInstanceId(30);
        snapshot.setNextRequestSeqNo(31);
        snapshot.setPartialResponseCache(new ArrayList<Reply>());
        snapshot.setStartingRequestSeqNo(32);
        snapshot.setValue(new byte[1024*1024*1024]);
        msgs.add(new CatchUpSnapshot(33, 34l, snapshot));

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