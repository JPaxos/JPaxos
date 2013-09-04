package lsr.paxos.test.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class BarrierClient
{

    public void enterBarrier(String host, int port, int requests, long time)
    {
        enterBarrier(host, port, requests, time, 1);
    }

    public void enterBarrier(String host, int port, int requests, long time,
                             int number)
    {
        try
        {
            Socket skt = new Socket(host, port);

            // System.out.println("# " + requests + " " + time);
            DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(skt.getOutputStream()));
            dos.writeInt(number);
            dos.writeInt(requests);
            dos.writeLong(time);
            dos.flush();

            BufferedReader in = new BufferedReader(new InputStreamReader(
                    skt.getInputStream()));

            while (!in.ready())
            {
            }
            in.readLine();
            in.close();
            dos.close();
            skt.close();
        } catch (Exception e)
        {
            // e.printStackTrace();
            System.out.print("Whoops! It didn't work!\n");
            System.exit(1);
        }
    }
}
