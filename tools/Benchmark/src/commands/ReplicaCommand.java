package commands;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import benchmark.MyLogger;
import benchmark.ProcessListener;
import benchmark.TestLoader;
import process_handlers.DigestServiceController;
import process_handlers.ReplicaHandler;
import process_handlers.ReplicaProcessController;

public class ReplicaCommand implements Command {

	Map<Integer, ReplicaHandler> replicas;

	public enum CommandType {
		Create, Stop
	};

	final int target;

	final String host;

	final String eventName;

	final CommandType type;

	ProcessListener listener;

	private final MyLogger logger;

	public ReplicaCommand(Map<Integer, ReplicaHandler> replicas, LinkedList<String> tokens, String eventName,
			ProcessListener listener, MyLogger logger) {
		this.eventName = eventName;
		// Start+0 Replica create 1 3
		// Starts replica no 1 on vnrunhost (3-1)
		this.listener = listener;
		this.replicas = replicas;
		this.logger = logger;

		if (tokens.size() < 2) {
			System.err.println("Wrong parameter count for replica command");
			throw new IllegalArgumentException();
		}

		String cmdType = tokens.removeFirst().toUpperCase();

		if (cmdType.equals("CREATE")) {
			if (tokens.isEmpty()) {
				System.err.println("Wrong parameter count for replica command");
				throw new IllegalArgumentException();
			}
			host = tokens.removeFirst();
		} else
			host = null;

		if (tokens.isEmpty()) {
			System.err.println("Wrong parameter count for replica command");
			throw new IllegalArgumentException();
		}

		String cmdTarget = tokens.removeFirst();
		try {
			if (cmdTarget.equals("*"))
				target = -1;
			else
				target = Integer.parseInt(cmdTarget);
		} catch (NumberFormatException e) {
			System.err.println("Wrong replica specification: " + cmdTarget);
			throw new IllegalArgumentException();
		}

		if (!tokens.isEmpty()) {
			System.err.println("Additional parameters for replica command: " + tokens.toString());
		}

		if (cmdType.equals("CREATE")) {
			type = CommandType.Create;
			if (target == -1) {
				System.err.println("Wrong replica specification: " + cmdTarget);
				throw new IllegalArgumentException();
			}
		} else if (cmdType.equals("STOP")) {
			type = CommandType.Stop;
		} else {
			System.err.println("Wrong replica command type: " + cmdType);
			throw new IllegalArgumentException();
		}
	}

	@Override
	public void execute() {
		switch (type) {
		case Create:
			if (replicas.get(target) != null) {
				System.err.println("Replica's already alive, ignoring command");
			} else {
				ReplicaHandler r = getReplicaHandler();
				// ReplicaProcessController r = new ReplicaProcessController(target, vnrunhost,
				// this, listener);
				r.setLastCommand(this);
				replicas.put(target, r);
				logger.replicaCreated(r);
			}
			break;

		case Stop:
			if (target == -1) {
				Collection<ReplicaHandler> rs = replicas.values();
				if (rs.size() == 0)
					logger.noSuchReplica(this);
				for (ReplicaHandler replica : rs) {
					replica.setLastCommand(this);
					replica.stop();
				}
				replicas.clear();
			} else {
				ReplicaHandler r = replicas.get(target);
				if (r == null) {
					logger.noSuchReplica(this);
				} else {
					r.setLastCommand(this);
					r.stop();
				}
				replicas.remove(target);
			}
			break;
		}
	}

	private ReplicaHandler getReplicaHandler() {
		switch (TestLoader.getReplicaHandler()) {
		case ReplicaProcessController:
			return (ReplicaHandler) new ReplicaProcessController(this.target, this.host, this, this.listener);
		case DigestServiceController:
			return (ReplicaHandler) new DigestServiceController(this.target, this.host, this, this.listener);
		}
		throw new RuntimeException("No such ReplicaHandlerType");
	}

	public String eventName() {
		return eventName;
	}

	public CommandType getType() {
		return type;
	}

	@Override
	public String toString() {

		String cmd = String.format("[Replica command] target: %10s, type: %6s",
				(target == -1 ? "all" : String.valueOf(target)), type.toString());

		if (type == CommandType.Create)
			cmd += String.format(", host: %s", host != null ? host : "(null)");

		return cmd;
	}
}
