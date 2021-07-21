package benchmark;

import java.io.PrintStream;

import commands.Command;
import commands.ReplicaCommand;
import process_handlers.ClientHandler;
import process_handlers.ProcessHandler;
import process_handlers.ReplicaHandler;

public class MyLogger {

	// [Replica command]

	Command lastCommand;

	final PrintStream out = System.out;

	enum LastMessages {
		Sent, Stop, Command, ClientCreated, Other
	};

	LastMessages lastMsg = LastMessages.Other;

	final boolean color = true;

	final String gray = "\033[01;30m";
	final String red = "\033[01;31m";
	final String green = "\033[01;32m";
	final String cyan = "\033[01;36m";

	final String inv_red = "\033[01;07;31m";

	final String norm = "\033[00m";

	private void ensureNewline() {
		if (lastMsg == LastMessages.Sent || lastMsg == LastMessages.Stop || lastMsg == LastMessages.ClientCreated) {
			out.println();
			out.flush();
		}
	}

	public synchronized void execute(Command c) {
		ensureNewline();

		if (color)
			out.print(green);

		lastMsg = LastMessages.Command;
		lastCommand = c;
		out.println("[Executing      ] " + c.toString());
		
		if (color)
			out.print(norm);
		out.flush();
	}

	public synchronized void execute(int numberOfTargets, Command c) {

		if (c != lastCommand || lastMsg != LastMessages.Command) {
			ensureNewline();
			if (color)
				out.print(green);
			out.println("[prev. command  ] " + c);
		}

		if (numberOfTargets == 0) {
			if (color)
				out.print(inv_red);
			out.printf("                  command applied to %3d processes !!!\n", numberOfTargets);
		} else {
			out.printf("                  command applied to %3d processes\n", numberOfTargets);
		}

		lastMsg = LastMessages.Other;
		if (color)
			out.print(norm);
		out.flush();
	}

	public synchronized void clientSent(ProcessHandler processHandler) {
		if (!(processHandler instanceof ClientHandler))
			return;
		ClientHandler client = (ClientHandler) processHandler;

		if (lastMsg != LastMessages.Sent) {
			ensureNewline();
			lastMsg = LastMessages.Sent;
			if (color)
				out.print(gray);
			out.print("[Send executed  ] " + client.getName());
		} else {
			out.print(" " + client.getName());
		}

		if (color)
			out.print(norm);
		out.flush();
	}

	public synchronized void eventOccured(String eventName) {
		ensureNewline();
		if (color)
			out.print(cyan);
		out.printf("[Event occured  ] %s\n", eventName);
		lastMsg = LastMessages.Other;

		if (color)
			out.print(norm);
		out.flush();
	}

	public synchronized void processStopped(ProcessHandler processHandler) {
		if (processHandler instanceof ReplicaHandler) {
			ensureNewline();
			if (color)
				out.print(red);
			lastMsg = LastMessages.Other;
			out.println("[Replica stopped] " + processHandler.toString());
		} else if (processHandler instanceof ClientHandler) {
			if (lastMsg != LastMessages.Stop) {
				ensureNewline();
				lastMsg = LastMessages.Stop;
				if (color)
					out.print(gray);
				out.print("[Client stopped ] " + ((ClientHandler) processHandler).getName());
			} else {
				out.print(" " + ((ClientHandler) processHandler).getName());
			}
		} else {
			ensureNewline();
			if (color)
				out.print(red);
			lastMsg = LastMessages.Other;
			out.println("[Unknown stopped] " + processHandler.toString());
		}
		if (color)
			out.print(norm);
		out.flush();
	}

	boolean finished = false;

	public synchronized void finished() {
		ensureNewline();
		finished = true;
		if (color)
			out.print(norm);
		out.println("Finished!");
		lastMsg = LastMessages.Other;
		if (color)
			out.print(norm);
		out.flush();
	}

	public synchronized void noSuchReplica(ReplicaCommand replicaCommand) {
		if (color)
			out.print(red);
		out.println("[    WARNING    ] No action taken for command " + replicaCommand.toString());
		lastMsg = LastMessages.Other;
		if (color)
			out.print(norm);
		out.flush();

	}

	public synchronized void errorCaught(ProcessHandler processHandler) {
		if (finished)
			return;
		ensureNewline();
		if (color)
			out.print(inv_red);
		lastMsg = LastMessages.Other;
		out.printf("[     ERROR     ] Process %s causerd error (last command: %s)\n", processHandler.toString(),
				processHandler.getLastCommand().toString());
		if (color)
			out.print(norm);
		out.flush();
	}

	public synchronized void clientCreated(ClientHandler client) {

		if (lastMsg != LastMessages.ClientCreated) {
			ensureNewline();
			lastMsg = LastMessages.ClientCreated;
			if (color)
				out.print(gray);
			out.print("[Client created ] " + client.getName());
		} else {
			out.print(" " + client.getName());
		}
		if (color)
			out.print(norm);
		out.flush();
	}

	public synchronized void replicaCreated(ReplicaHandler processHandler) {
		ensureNewline();
		if (color)
			out.print(red);
		lastMsg = LastMessages.Other;
		out.println(
				"[Replica started] " + processHandler.getLocalId() + " (" + processHandler.getLaunchCommand() + ")");
		if (color)
			out.print(norm);
		out.flush();
	}

}
