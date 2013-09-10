package lsr.paxos.test.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import lsr.common.PID;
import lsr.paxos.client.Client;

public class SimpleClient extends Client {

    private String host;
    private int port;
    private Socket socket;
    private DataOutputStream outStream;
    private DataInputStream inStream;

    private static List<PID> getReplicaList(String host, int port) {
        List<PID> list = new LinkedList<PID>();
        list.add(new PID(0, host, port, port));
        return list;
    }

    @SuppressWarnings("deprecation")
    public SimpleClient(String host, int port) throws IOException {
        super(getReplicaList(host, port));
        this.host = host;
        this.port = port;
    }

    @Override
    public void connect() {
        try {
            socket = new Socket(host, port);
            outStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            inStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void disconnect() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] execute(byte[] bytes) {
        try {
            outStream.writeInt(bytes.length);
            outStream.write(bytes);
            outStream.flush();

            int bytesToRead = inStream.readInt();
            byte[] msg = new byte[bytesToRead];

            int bytesRead = 0;
            while (bytesRead != bytesToRead) {
                int x = inStream.read(msg, bytesRead, bytesToRead - bytesRead);
                if (x == -1)
                    throw new IOException("invalid message");
                bytesRead += x;
            }

            return msg;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
