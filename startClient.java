
import java.io.*;
import java.lang.Runtime;
import lsr.paxos.test.EchoClient;

public class startClient {
	
	public static void main(String[] args) {
		
		String clients = "";
		File file = new File("test_nodes.sh");
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		DataInputStream dis = null;
	 
	 try {
			fis = new FileInputStream(file);
			bis = new BufferedInputStream(fis);
			dis = new DataInputStream(bis);
			
			dis.readLine();
			clients = dis.readLine();
			int index = (clients.indexOf('"')) + 1;
			int next = index;
			int client = 0;
			
			while(index < clients.length()){
				next = clients.indexOf(' ',index);
				if (next < index || next > clients.length()) next = (clients.length()) - 1;
				client = Integer.parseInt(clients.substring(index,next));
				Thread thread = new Thread(new startClientThread());
				thread.start();
				try {
					Thread.sleep(1);
				} catch (InterruptedException e1) {}
				index = next + 1;
			}
			
			fis.close();
			bis.close();
			dis.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
				
	}
}
