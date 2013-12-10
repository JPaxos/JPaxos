package process_handlers;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import benchmark.ProcessListener;
import benchmark.TestLoader;

import commands.Command;
import commands.ReplicaCommand;
import commands.ReplicaCommand.CommandType;

public class ReplicaProcessController implements ReplicaHandler {

	final public String launchCommand;
	final public int numberOfReplica;
	
	private ReplicaCommand lastCommand;
	private ProcessListener listener;
	private OutputStream os;
	private OutputStream file_bos;
	private Process process;
	private boolean stopping;
	
	public String toString() {
		return String.valueOf(numberOfReplica);
	}
	
	
	private class InputStreamReader implements Runnable {
		private final InputStream source;
		private final OutputStream sink;
		
		public InputStreamReader(InputStream is, OutputStream os) {
			this.source = is;
			this.sink = os;
		}

		@Override
		public void run() {
			byte[] b = new byte[1024 * 8];
			try {
				int count = source.read(b);
				while (count != -1) {
					sink.write(b, 0, count);
//					sink.flush();
					count = source.read(b);						
//					System.out.println(Thread.currentThread().getName() +  ":Read: " + count);
				}
			} catch (IOException e) {
				if (!stopping)
					throw new RuntimeException("Process could not log output", e);
			}
			
		}
	}

	public ReplicaProcessController(final int target, int vnrunhost, ReplicaCommand lastCommand,
			ProcessListener _listener) {

		numberOfReplica = target;
		this.lastCommand = lastCommand;
		listener = _listener;

		String cmd = TestLoader.getReplicaCmd();
		int replicaNo = vnrunhost - 1;
		cmd = cmd.replaceAll("MODEL", TestLoader.getModelnetFile());
		cmd = cmd.replaceAll("NUM", String.valueOf(target - 1));
		cmd = cmd.replaceAll("VNODE", String.valueOf(replicaNo));		
		launchCommand = cmd;
		
		try {
			process = Runtime.getRuntime().exec(launchCommand);			
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		final ReplicaProcessController me = this;
		os = process.getOutputStream();		
		listener.processCreated(me);
		try {
			file_bos = 
				//new BufferedOutputStream(
					new FileOutputStream("replica__" + target + ".log", true);
					//1024*16);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			System.exit(1);
		}

		// Separate threads to read the stdout and stderr
		new Thread(new InputStreamReader(process.getInputStream(), file_bos),
				"Replica-" + replicaNo + "-stdout").start();
		new Thread(new InputStreamReader(process.getErrorStream(), file_bos),
				"Replica-" + replicaNo + "-stderr").start();
		
		// Waits until the process exits.
		new Thread("Replica-" + replicaNo) {
			public void run() {
				try {
					process.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (listener != null) {
					if (process.exitValue() == 0)
						listener.processFinished(me);
					else if ((me.lastCommand).getType() == CommandType.Stop) {
						listener.processFinished(me);
					} else {
						listener.errorCaught(me);
					}
				}

			}
		}.start();
	}

	private void flushFileOut() {
		try {
			file_bos.flush();
		} catch (IOException e1) {
		}
	}

	@Override
	public void addProcessListener(ProcessListener procesListener) {
		if (listener != null)
			throw new RuntimeException();
		listener = procesListener;
	}

	@Override
	public void kill() {
		stopping = true;
		process.destroy();
		flushFileOut();
	}

	@Override
	public void stop() {
		stopping = true;
		try {
			// A newline is interpreted by the replica client as a sign to stop.
			os.write('\n');
			os.flush();
		} catch (IOException e) {
			process.destroy();
		}

		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
			flushFileOut();
			System.exit(1);
		}

		try {
			process.exitValue();
		} catch (IllegalThreadStateException e) {
			process.destroy();
		}

		flushFileOut();

	}

	public Command getLastCommand() {
		return lastCommand;
	}

	public void setLastCommand(Command c) {
		if (c instanceof ReplicaCommand)
			lastCommand = (ReplicaCommand) c;

	}

}
