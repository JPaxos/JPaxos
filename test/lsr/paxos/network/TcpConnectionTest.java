package lsr.paxos.network;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import lsr.common.Configuration;
import lsr.common.PID;
import lsr.common.ProcessDescriptor;

import org.junit.Before;
import org.junit.Test;

public class TcpConnectionTest {

    private PID pid0;
    private PID pid1;
    private PID pid2;

    @Before
    public void setUp() {
        pid0 = new PID(0, "localhost", 2000, 3000);
        pid1 = new PID(1, "localhost", 2001, 3001);
        pid2 = new PID(2, "localhost", 2002, 3002);
        List<PID> processes = new ArrayList<PID>();
        processes.add(pid0);
        processes.add(pid1);
        processes.add(pid2);

        Configuration configuration = new Configuration(processes);
        ProcessDescriptor.initialize(configuration, 1);
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void activeConnectionShouldSendLocalId() throws Exception {
        TcpNetwork network = mock(TcpNetwork.class);
        boolean active = true;
        TcpConnection connection = new TcpConnection(network, pid2, active);
        connection.start();

        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress((InetAddress) null, pid2.getReplicaPort()));
        Socket socket = server.accept();
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        assertEquals(1, inputStream.readInt());
        inputStream.close();
        socket.close();
        server.close();

        connection.stop();
    }

    @Test(timeout = 2000)
    public void shouldSendMessage() throws Exception {
        TcpNetwork network = mock(TcpNetwork.class);
        boolean active = true;
        TcpConnection connection = new TcpConnection(network, pid2, active);
        connection.start();

        // handle connect
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress((InetAddress) null, pid2.getReplicaPort()));
        Socket socket = server.accept();
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        assertEquals(1, inputStream.readInt());

        // send new message
        connection.send(new byte[] {1, 2, 3, 4});
        byte[] message = new byte[4];
        inputStream.readFully(message);
        assertArrayEquals(new byte[] {1, 2, 3, 4}, message);

        inputStream.close();
        socket.close();
        server.close();

        connection.stop();
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout = 2000)
    public void activeConnectionShouldNotBlockSendMethodWhenNotConnected() throws Exception {
        TcpNetwork network = mock(TcpNetwork.class);
        boolean active = true;
        TcpConnection connection = new TcpConnection(network, pid2, active);
        connection.start();

        Thread.sleep(100);
        for (int i = 0; i < 200; i++) {
            connection.send(new byte[] {0, 1, 2, 3});
        }
        Thread.sleep(100);

        connection.stop();
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(timeout = 2000)
    public void passiveConnectionShouldNotBlockSendMethodWhenNotConnected() throws Exception {
        TcpNetwork network = mock(TcpNetwork.class);
        boolean active = false;
        TcpConnection connection = new TcpConnection(network, pid2, active);
        connection.start();

        Thread.sleep(100);
        for (int i = 0; i < 200; i++) {
            connection.send(new byte[] {0, 1, 2, 3});
        }
        Thread.sleep(100);

        connection.stop();
    }

    @Test(timeout = 2000)
    public void shouldIgnoreMessagesSentBeforeConnect() throws Exception {
        TcpNetwork network = mock(TcpNetwork.class);
        boolean active = true;
        TcpConnection connection = new TcpConnection(network, pid2, active);
        connection.start();
        connection.send(new byte[] {1, 2, 3, 4});

        // handle connect
        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress((InetAddress) null, pid2.getReplicaPort()));
        Socket socket = server.accept();
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        assertEquals(1, inputStream.readInt());

        // send new message
        connection.send(new byte[] {5, 6, 7, 8});
        byte[] message = new byte[4];
        inputStream.readFully(message);
        assertArrayEquals(new byte[] {5, 6, 7, 8}, message);

        inputStream.close();
        socket.close();
        server.close();

        connection.stop();
    }
}
