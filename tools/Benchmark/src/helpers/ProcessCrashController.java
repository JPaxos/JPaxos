package helpers;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;

/**
 * Controls the number of crashes so no more than a minority of
 * processes are crashed simultaneously. Before crashing, 
 * processes request permission to crash. It is granted only if 
 * the crash doesn't violate model assumptions. Processes also 
 * inform when they recover.
 * 
 *  TODO: Change the protocol, so that it's this class that decides
 *  when a process should crash and informs the processes. This
 *  allows centralizing the logic for controlling the crashes,
 *  instead of doing it in the replicas.
 *  
 * @author Nuno Santos (LSR)
 */
public class ProcessCrashController implements Runnable {
	
	public static final int PORT = 5678; 

	private final Object lock = new Object();
	private BitSet processState;
	private int replicaCount = -1;

	public static enum Type {Initialize, Crash, Recover, Grant, Deny};

	public static class Request implements Serializable {
		public final int replicaCount;
		public final int id;
		public final Type type;

		public Request(Type type, int replicaNo, int replicaCount) {
			this.type = type;
			this.id = replicaNo;
			this.replicaCount = replicaCount;
		}
	}
	
	public static class Reply implements Serializable {
		public final Type type;
		public final String msg;
		public Reply(Type type, String msg) {
			this.type = type;
			this.msg = msg;
		}
		public Reply(Type type) {
			this(type, null);
		}
	}

	class ClientConnection implements Runnable {
		private final Socket s;
		private ObjectInputStream ois;
		private ObjectOutputStream oos;
		
		private int pid = -1;

		public ClientConnection(Socket s) throws IOException {
			this.s = s;
			// If the order is reversed (first ois then oos), 
			// then getInputStream() blocks. (???)
			this.oos = new ObjectOutputStream(s.getOutputStream());
			this.ois = new ObjectInputStream(s.getInputStream());
		}

		@Override
		public void run() {

			try {
				while (true) {
					Request req = (Request) ois.readObject();
					assert pid == -1 || req.id == pid : "Process pid changed? Expected: " + pid + ", received: " + req.id;

					synchronized (lock) {
						switch (req.type) {
						case Initialize:
							if (processState == null) {
								// All processes are up
								processState = new BitSet(req.replicaCount);
								replicaCount = req.replicaCount;
								System.out.println("Initialized to " + req.replicaCount + " nodes");
							}

							if (replicaCount != req.replicaCount) {
								String error = "Replica count mismatch. Requested: " + req.replicaCount + ", " +
									"previous: " + replicaCount;
								oos.writeObject(new Reply(Type.Deny, error));
								System.out.println(error);
								break;
							} 
							
							pid = req.id;
							processState.set(pid);				
							oos.writeObject(new Reply(Type.Grant));
							System.out.println("Process: " + pid + " initialized");
							break;

						case Crash:
							if (processState == null) {
								oos.writeObject(new Reply(Type.Deny, "Not initialized"));
								break;
							}

							if (!processState.get(req.id)) {
								oos.writeObject(new Reply(Type.Deny, "Process " + req.id + " is already crashed"));
								break;
							} 

							int alive = processState.cardinality();
							if (alive-1  <= replicaCount / 2) {
								oos.writeObject(new Reply(Type.Deny, "Maximum crashes already reached. Alive: " + alive));
								break;
							}

							// Process is allowed to crash
							processState.flip(req.id);
							assert !processState.get(req.id);
							oos.writeObject(new Reply(Type.Grant));
							System.out.println("Process " + req.id + " crashed");
							break;

						case Recover:
							if (processState == null) {
								oos.writeObject(new Reply(Type.Deny, "Not initialized"));
								break;
							}

							if (processState.get(req.id)) {
								oos.writeObject(new Reply(Type.Deny, "Process " + req.id + " is already alive"));
								break;
							}

							System.out.println("Process " + req.id + " recovered");
							processState.flip(req.id);
							oos.writeObject(new Reply(Type.Grant));
							break;

						default:
							throw new AssertionError("Illegal request type: " + req.type); 
						}
					}
					oos.flush();
				}
			} catch (Exception e) {
				System.out.println("Process " + s.getInetAddress() + " disconnected");
			}
		}
	}	

	ServerSocket ss;
	public ProcessCrashController(int listenPort) throws IOException {
		ss = new ServerSocket(listenPort);		
	}

	public void run()  {
		System.out.println("Waiting for connections on port: " + ss.getLocalPort());
		while (true) {
			try {
				Socket s = ss.accept();
				System.out.println("New connection received: " + s.getInetAddress());				
				Thread t = new Thread(new ClientConnection(s));
				t.start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		ProcessCrashController server = new ProcessCrashController(PORT);
		new Thread(server).start();		
	}
}
