package benchmark;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import process_handlers.ClientHandler;
import process_handlers.ProcessHandler;
import process_handlers.ReplicaHandler;
import commands.Command;

public class Benchmark {

	private List<ClientHandler> clients = Collections.synchronizedList(new Vector<ClientHandler>());
	private Map<Integer, ReplicaHandler> replicas = Collections.synchronizedMap(new HashMap<Integer, ReplicaHandler>());

	private SortedMap<Long, List<Command>> tasks = Collections
			.synchronizedSortedMap(new TreeMap<Long, List<Command>>());

	private Scheduler scheduler;

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 1) {
			System.err.println("No test file given");
			System.exit(1);
		}

		File test = new File(args[0]);
		FileReader testReader = null;

		try {
			testReader = new FileReader(test);
		} catch (FileNotFoundException e) {
			System.err.println("File " + args[0] + " inaccessible");
			System.exit(1);
		}

		MyLogger logger = new MyLogger();

		Benchmark benchmark = new Benchmark(logger);

		ProcessListener listener = new DefaultProcessListener(benchmark, benchmark.scheduler, logger);

		TestLoader reader = new TestLoader(testReader, benchmark.scheduler, benchmark.clients, benchmark.replicas,
				listener, logger);

		reader.parse();

		benchmark.run();

	}

	private void run() throws InterruptedException {
		while (true) {
			executeTasks();
			if (clients.isEmpty() && tasks.isEmpty() && replicas.isEmpty())
				break;
			if (tasks.isEmpty())
				synchronized (this) {
					wait();
				}
			else
				synchronized (this) {
					wait(Math.max(1, tasks.firstKey() - System.currentTimeMillis()));
				}
		}
		logger.finished();
		if (!scheduler.isEmpty()) {
			System.err.println("Warning: some tasks were left unscheduled!");
			scheduler.printWaitingTasks();
		}
		stop();
		System.exit(1);

	}

	private void executeTasks() {
		while (!tasks.isEmpty() && tasks.firstKey() <= System.currentTimeMillis()) {
			List<Command> list;
			synchronized (tasks) {
				long key = tasks.firstKey();
				list = tasks.remove(key);
			}
			for (Command c : list) {
				logger.execute(c);
				c.execute();
			}
		}
	}

	public void scheduleChanged() {
		synchronized (this) {
			notify();
		}
	}

	boolean exiting = false;
	private final MyLogger logger;

	public void forceStop() {
		if (exiting)
			return;
		stop();
		System.exit(1);
	}

	private void stop() {
		exiting = true;

		synchronized (clients) {
			for (ClientHandler clientHandler : clients)
				clientHandler.kill();
		}
		synchronized (replicas) {
			for (ReplicaHandler replicaHandler : replicas.values())
				replicaHandler.kill();
		}
	}

	public Benchmark(MyLogger logger) {
		this.logger = logger;
		scheduler = new DefaultScheduler(this, tasks, logger);

	}

	@SuppressWarnings("unlikely-arg-type")
	public void processStopped(ProcessHandler processHandler) {
		if (exiting)
			return;
		clients.remove(processHandler);
		replicas.remove(processHandler);
		synchronized (this) {
			notify();
		}

	}

}
