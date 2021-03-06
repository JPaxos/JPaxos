package benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import commands.ClientCommand;
import commands.Command;
import commands.ReplicaCommand;
import process_handlers.ClientHandler;
import process_handlers.ReplicaHandler;

public class TestLoader {

	private FileReader testReader;
	private Scheduler scheduler;
	private List<ClientHandler> clients;
	private Map<Integer, ReplicaHandler> replicas;
	private ProcessListener listener;

	private static String configFile = null;
	private static String replicaCmd = null;
	private static String clientCmd = null;

	public enum ReplicaHandlerType {
		ReplicaProcessController, DigestServiceController;
	}

	private static ReplicaHandlerType replicaHandler = null;

	public static ReplicaHandlerType getReplicaHandler() {
		return replicaHandler;
	}

	public static String getConfigFile() {
		return configFile;
	}

	public static String getReplicaCmd() {
		return replicaCmd;
	}

	public static String getClientCmd() {
		return clientCmd;
	}

	String line = null;
	private final MyLogger logger;

	public TestLoader(FileReader testReader, Scheduler scheduler, List<ClientHandler> clients,
			Map<Integer, ReplicaHandler> replicas, ProcessListener listener2, MyLogger logger) {
		this.listener = listener2;
		this.testReader = testReader;
		this.clients = clients;
		this.replicas = replicas;
		this.scheduler = scheduler;
		this.logger = logger;
	}

	class MyBufferedReader extends BufferedReader {
		public MyBufferedReader(Reader in) {
			super(in);
		}

		@Override
		public String readLine() throws IOException {
			String s;
			do {
				s = super.readLine();
				if (s == null)
					return null;
				s = simplifyWhitespaces(s);
			} while (s.matches("^//.*") || s.matches("^#.*") || s.isEmpty());
			return s;
		}

		private String simplifyWhitespaces(String s) {
			return s.replaceFirst("^[ \t][ \t]*", "").replaceFirst("[ \t][ \t]*$", "").replaceAll("[ \t][ \t]*", " ");
		}
	}

	public void parse() {
		MyBufferedReader testBufferedReader = new MyBufferedReader(testReader);

		// Config file:
		// 1) replica command
		// 2) client command
		// 3) config file
		// Variable settings:
		// A) Client:
		// CONFIG HOST NAME
		// B) Replica:
		// CONFIG HOST NUM

		try {
			replicaCmd = testBufferedReader.readLine();
			clientCmd = testBufferedReader.readLine();
			configFile = testBufferedReader.readLine();
			replicaHandler = ReplicaHandlerType.valueOf(testBufferedReader.readLine());
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		while (true) {
			try {
				line = testBufferedReader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

			if (line == null)
				break;

			String line = this.line.toUpperCase();

			LinkedList<String> tokens = new LinkedList<String>();
			tokens.addAll(Arrays.asList(line.split("[\t ]")));

			String time = tokens.removeFirst();

			try {
				scheduler.addTask(time, readCommand(tokens));
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		try {
			testBufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static List<String> noEventNames = new Vector<String>();
	{
		noEventNames.add("NONE");
		noEventNames.add("OFF");
		noEventNames.add("NO");
		noEventNames.add("FALSE");
	}

	private Command readCommand(LinkedList<String> tokens) {

		String type = tokens.removeFirst().toUpperCase();

		String eventName = tokens.removeLast().toUpperCase();

		if (eventName.contains(",")) {
			System.err.println("Event name cannot have a ',' character");
			System.exit(1);
		}

		if (noEventNames.contains(eventName))
			eventName = null;

		if (type.equals("CLIENT")) {
			return new ClientCommand(clients, tokens, eventName, listener, logger);
		} else if (type.equals("REPLICA")) {
			return new ReplicaCommand(replicas, tokens, eventName, listener, logger);
		}

		System.err.print("Wrong command type: " + type + " May be either Replica, or Client:\n" + line);
		System.exit(1);

		return null;
	}
}
