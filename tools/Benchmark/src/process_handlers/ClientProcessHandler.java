package process_handlers;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import benchmark.TestLoader;
import benchmark.ProcessListener;

import commands.ClientCommand;
import commands.Command;
import commands.ClientCommand.CommandType;

public class ClientProcessHandler implements ClientHandler {

	ClientCommand lastCommand;

	ProcessListener listener = null;

	Process process;

	String name = null;

	final int vnrunhost;

	public ClientProcessHandler(int vnrunhost) {
		System.err.println("Old client!!!");
		System.exit(1);
		this.vnrunhost = vnrunhost;
	}

	@Override
	public void sendRequests(int count, long delay, boolean randomDelay) {

		String cmd = TestLoader.getClientCmd();

		cmd = cmd.replaceFirst("MODEL", TestLoader.getModelnetFile());
		cmd = cmd.replaceFirst("COUNT", String.valueOf(count));
		cmd = cmd.replaceFirst("DELAY", String.valueOf(delay));
		cmd = cmd.replaceFirst("RANDOM", String.valueOf(randomDelay));
		cmd = cmd.replaceFirst("VNODE", String.valueOf(vnrunhost));

		try {
			process = Runtime.getRuntime().exec(cmd);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		final ClientProcessHandler me = this;

		new Thread() {
			public void run() {

				// InputStream out = process.getInputStream();
				InputStream out = process.getErrorStream();

				FileOutputStream fis = null;
				try {
					fis = new FileOutputStream("client__" + me.name);
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
					System.exit(1);
				}

				int b;
				try {
					while (true) {
						b = out.read();
						if (b != -1)
							fis.write(b);
						else
							break;
					}
				} catch (IOException e) {
				}

				try {
					process.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (listener != null) {
					if (process.exitValue() == 0) {
						listener.clientSent(me);
					} else {
						if (lastCommand.getType() == CommandType.Stop) {
							listener.clientSent(me);
						} else
							listener.errorCaught(me);
					}
				}

			}
		}.start();

	}

	@Override
	public void addProcessListener(ProcessListener procesListener) {
		if (listener != null)
			throw new RuntimeException();
		listener = procesListener;

	}

	@Override
	public void kill() {
		process.destroy();
		listener.processFinished(this);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public void stop() {
		process.destroy();
		listener.processFinished(this);
	}

	public Command getLastCommand() {
		return lastCommand;
	}

	public void setLastCommand(Command c) {
		if (c instanceof ClientCommand)
			lastCommand = (ClientCommand) c;

	}

}
