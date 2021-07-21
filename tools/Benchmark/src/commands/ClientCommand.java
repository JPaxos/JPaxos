package commands;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import benchmark.MyLogger;
import benchmark.ProcessListener;
import process_handlers.BenchmarkClientHandler;
import process_handlers.ClientHandler;

public class ClientCommand implements Command {

	public String toString() {

		String cmd = String.format("[Client  command] target: %10s, type: %6s", target, type.toString());

		switch (type) {
		case Create:
			cmd += String.format(", host: %s , count: %5d", ((Create) data).host, ((Create) data).count);
			break;
		case Send:
			Send s = (Send) data;
			cmd += String.format(", count: %5d, delay: %5d %-10s, event: %s", s.count, s.delay,
					(s.randomDelay ? "(random)" : "(deterministic)"), eventName);
			break;
			// remaining commands have no extra text
		case Kill:
		case Stop:
		}

		return cmd;
	}

	List<ClientHandler> clients;

	public enum CommandType {
		Create, Stop, Send, Kill;
	};

	String target = null;

	final String eventName;

	final CommandType type;

	ProcessListener listener;

	final Object data;

	private final MyLogger logger;

	public ClientCommand(List<ClientHandler> clients, LinkedList<String> tokens, String eventName,
			ProcessListener listener, MyLogger logger) {
		this.eventName = eventName;
		this.listener = listener;
		this.clients = clients;
		this.logger = logger;

		if (tokens.size() < 2)
			throw new IllegalArgumentException();

		target = tokens.removeFirst();

		String cmdType = tokens.removeFirst().toUpperCase();

		if (cmdType.equals("CREATE")) {
			type = CommandType.Create;
			data = new Create(tokens);
		} else if (cmdType.equals("STOP")) {
			type = CommandType.Stop;
			data = new Stop(tokens);
		} else if (cmdType.equals("SEND")) {
			type = CommandType.Send;
			data = new Send(tokens);
		} else if (cmdType.equals("KILL")) {
			this.type = CommandType.Kill;
			this.data = new Kill(tokens);
		} else {
			System.err.println("Wrong command type: " + cmdType);
			throw new IllegalArgumentException();
		}

	}

	public int numberOfTargets = 1;

	public void execute() {
		List<ClientHandler> targetList;
		switch (type) {
		case Create:
			execCreate((Create) data);
			break;
		case Stop:
			targetList = getClientList(target);
			numberOfTargets = targetList.size();
			logger.execute(numberOfTargets, this);
			// if (targetList.isEmpty()) {
			// System.err.println("Useless command executed!");
			// }
			for (ClientHandler ch : targetList) {
				ch.setLastCommand(this);
				ch.stop();
			}
			targetList.clear();
			break;
		case Send:
			targetList = getClientList(target);
			numberOfTargets = targetList.size();
			logger.execute(numberOfTargets, this);
			execSend((Send) data, targetList);
			break;
		case Kill:
			targetList = getClientList(target);
			numberOfTargets = targetList.size();
			logger.execute(numberOfTargets, this);
			for (ClientHandler ch : targetList) {
				ch.setLastCommand(this);
				ch.kill();
			}
			targetList.clear();
			break;
		}

	}

	private void execSend(Send data, List<ClientHandler> targetList) {
		// if (targetList.isEmpty()) {
		// System.err.println("Useless command executed!");
		// }

		for (ClientHandler client : targetList) {
			client.setLastCommand(this);
			client.sendRequests(data.count, data.delay, data.randomDelay);
		}
	}

	private List<ClientHandler> getClientList(String target) {
		List<ClientHandler> list = new Vector<ClientHandler>();
		synchronized (clients) {
			for (ClientHandler h : clients)
				if (h.getName().matches(target))
					list.add(h);
		}

		return list;
	}

	private void execCreate(Create data) {
		if (data.single) {
			numberOfTargets = 1;
			ClientHandler client = new BenchmarkClientHandler(data.host, target, this, listener);
			clients.add(client);
			client.setLastCommand(this);
			logger.clientCreated(client);
		} else {
			numberOfTargets = data.count;
			for (int i = 0; i < data.count; ++i) {
				ClientHandler client = new BenchmarkClientHandler(data.host, target + String.valueOf(i), this,
						listener);
				clients.add(client);
				client.setLastCommand(this);
				logger.clientCreated(client);
			}
		}
	}

	public String eventName() {
		return eventName;
	}

	private class Create {
		// start+0 client prefix create 6 2
		// Creates 2 clients on vnrunhost (6-1)
		boolean single = false;
		int count = 1;

		final String host;

		public Create(LinkedList<String> tokens) {

			if (tokens.size() == 0) {
				System.err.println("Not enought parameters for create command");
				throw new IllegalArgumentException();
			}

			host = tokens.removeFirst();

			if (tokens.isEmpty()) {
				single = true;
			} else {
				try {
					count = Integer.parseInt(tokens.getFirst());
					if (tokens.size() != 1) {
						tokens.removeFirst();
						System.err.println("Additional parameters: " + tokens.toString());
					}
				} catch (NumberFormatException e) {
					System.err.println("Wrong count: " + tokens.getFirst());
					throw new IllegalArgumentException(e);
				}
			}
		}
	}

	private class Kill {
		public Kill(LinkedList<String> tokens) {
		}
	}

	private class Stop {
		// start+0 client boom* kill event

		public Stop(LinkedList<String> tokens) {
		}

	}

	private class Send {
		// start+0 client foo send [ count [ delay [ RANDOM]]] event
		int count = 1;
		long delay = 0;
		boolean randomDelay = false;

		public Send(LinkedList<String> tokens) {

			if (!tokens.isEmpty()) {

				String cmdCount = tokens.removeFirst();
				try {
					count = Integer.parseInt(cmdCount);
				} catch (NumberFormatException e) {
					System.err.println("Wrong count: " + cmdCount);
					throw new IllegalArgumentException(e);
				}

				if (!tokens.isEmpty()) {

					String cmdDelay = tokens.removeFirst();
					try {
						delay = Long.parseLong(cmdDelay);
					} catch (NumberFormatException e) {
						System.err.println("Wrong count: " + cmdDelay);
						throw new IllegalArgumentException(e);
					}
					if (!tokens.isEmpty()) {
						String rnd = tokens.removeFirst().toUpperCase();
						if (rnd.equals("RANDOM"))
							randomDelay = true;
						else {
							System.err.println("Wrong parameter!");
							throw new IllegalArgumentException();
						}

						if (tokens.size() != 0) {
							System.err.println("Additional parameters: " + tokens.toString());
						}
					}
				}
			}
		}
	}

	public CommandType getType() {
		return type;
	}

}
