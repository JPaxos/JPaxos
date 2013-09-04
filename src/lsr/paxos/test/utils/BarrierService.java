package lsr.paxos.test.utils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BarrierService
{
    private static long lastBarrierTime = System.currentTimeMillis();
    private static long totalTime;
    private static int totalTransactions;

    public static class RequestHandlerThread extends Thread
    {
        private Socket socket;

        public static Object waitLock = new Object();
        public static int numberOfWaiting = 0;
        public ServerThread serverThread;

        public RequestHandlerThread(Socket socket, ServerThread serverThread)
        {
            this.socket = socket;
            this.serverThread = serverThread;
        }

        public void run()
        {
            PrintWriter out;
            DataInputStream dis;
            try
            {
                dis = new DataInputStream(new BufferedInputStream(
                        socket.getInputStream()));
                synchronized (waitLock)
                {
                    numberOfWaiting += dis.readInt();
                    totalTransactions += dis.readInt();
                    totalTime += dis.readLong();
                    // numberOfWaiting++;
                    waitLock.wait();
                }

                out = new PrintWriter(socket.getOutputStream(), true);
                out.println(true);
                out.close();
                socket.close();
            } catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static class ServerThread extends Thread
    {
        int port;
        ServerSocket srvr;

        public ServerThread(int port)
        {
            this.port = port;
        }

        public void run()
        {
            try
            {
                srvr = new ServerSocket(port);
                while (true)
                {
                    Socket socket = srvr.accept();
                    Thread t = new RequestHandlerThread(socket, this);
                    t.start();
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) throws InterruptedException,
            IOException
    {
        int numberOfClients = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        int counter = 0;

        ServerThread serverThread = new ServerThread(port);
        serverThread.start();

        // try
        // {
        while (true)
        {
            // Vector<Thread> threads = new Vector<Thread>();
            // for (int i = 0; i < 1; i++)
            // {
            // Socket socket = srvr.accept();
            // Thread t = new RequestHandlerThread(socket, this);
            // threads.add(t);
            // t.start();
            // }

            synchronized (RequestHandlerThread.waitLock)
            {

                while (RequestHandlerThread.numberOfWaiting != numberOfClients)
                {
                    RequestHandlerThread.waitLock.wait(100);
                }
                long newTime = System.currentTimeMillis();
                if (counter % 2 == 1)
                    System.out.println(totalTime
                                       / RequestHandlerThread.numberOfWaiting + "\t\t"
                                       + totalTransactions + "\t\t" + 1000f
                                       * totalTransactions / totalTime
                                       * RequestHandlerThread.numberOfWaiting + "\t\t\t" +
                                       (newTime - lastBarrierTime) / 1000.0f);
                RequestHandlerThread.numberOfWaiting = 0;
                totalTime = 0;
                totalTransactions = 0;
                lastBarrierTime = newTime;
                RequestHandlerThread.waitLock.notifyAll();
            }

            // long newTime = System.currentTimeMillis();
            // if (counter % 2 == 1)
            // System.out.println((newTime - lastBarrierTime) / 1000.0f);
            // lastBarrierTime = newTime;

            // for (Thread thread : threads)
            // {
            // thread.join();
            // }
            counter++;
        }
        // }
        // finally
        // {
        // srvr.close();
        // }
    }

}
