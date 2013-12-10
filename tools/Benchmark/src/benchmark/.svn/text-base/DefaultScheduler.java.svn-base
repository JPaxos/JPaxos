package benchmark;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Vector;
import java.util.Map.Entry;

import commands.*;

public class DefaultScheduler implements Scheduler {

	private Map<List<String>, Map<Long, List<Command>>> eventTaskMap = new HashMap<List<String>, Map<Long, List<Command>>>();
	private Benchmark benchmark;
	private SortedMap<Long, List<Command>> tasks;
	private final MyLogger logger;

	public DefaultScheduler(Benchmark benchmark, SortedMap<Long, List<Command>> tasks, MyLogger logger) {
		this.tasks = tasks;
		this.benchmark = benchmark;
		this.logger = logger;
	}

	@Override
	public void addTask(String time, Command command) {
		String timet[] = time.split("\\+");
		if (timet.length != 2) {
			System.err.println("Time definition error in \"" + time + "\"");
			System.exit(1);
		}

		Long shift = null;
		try {
			shift = Long.parseLong(timet[1]);
		} catch (NumberFormatException e) {
			System.err.println("Time definition error in \"" + time + "\"");
			System.exit(1);
		}

		if (timet[0].toUpperCase().equals("START")) {
			scheadule(shift, command);
		} else {
			addToTimeMap(timet[0], shift, command);
		}

	}

	private void addToTimeMap(String event, Long shift, Command command) {
		String[] eventt = event.split(",");
		Vector<String> eventv = new Vector<String>();
		Collections.sort(eventv);

		for (String s : eventt) {
			eventv.add(s);
		}

		Map<Long, List<Command>> taskMap = eventTaskMap.get(eventv);
		if (taskMap == null) {
			taskMap = new TreeMap<Long, List<Command>>();
			eventTaskMap.put(eventv, taskMap);
		}

		List<Command> list = taskMap.get(shift);
		if (list == null) {
			list = new Vector<Command>();
			taskMap.put(shift, list);
		}

		list.add(command);

	}

	private void scheadule(Long shift, Command command) {
		long time = System.currentTimeMillis() + shift;
		List<Command> list = tasks.get(time);
		if (list == null) {
			list = new Vector<Command>();
			tasks.put(time, list);

		}
		list.add(command);

		if (tasks.firstKey() == time) {
			benchmark.scheduleChanged();
		}
	}

	public synchronized boolean isEmpty() {
		return eventTaskMap.isEmpty();
	}

	private List<List<String>> findAllOccurences(String event) {
		List<List<String>> result = new Vector<List<String>>();
		for (Entry<List<String>, Map<Long, List<Command>>> entry : eventTaskMap.entrySet()) {
			if (entry.getKey().contains(event)) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	public synchronized void eventOccured(String event) {

		Long currentTime = System.currentTimeMillis();

		logger.eventOccured(event);

		for (List<String> eventList : findAllOccurences(event)) {

			Map<Long, List<Command>> tasksWith = eventTaskMap.get(eventList);

			List<String> newEventList = new Vector<String>();
			newEventList.addAll(eventList);

			while (newEventList.remove(event))
				;

			if (newEventList.isEmpty()) {
				eventTaskMap.remove(eventList);

				scheaduleList(tasksWith, currentTime);

				continue;
			}

			Map<Long, List<Command>> tasksWithout = eventTaskMap.get(newEventList);

			if (tasksWithout == null) {
				eventTaskMap.remove(eventList);
				eventTaskMap.put(newEventList, tasksWith);
			} else {
				for (Entry<Long, List<Command>> entry : tasksWith.entrySet()) {
					List<Command> commands = tasksWithout.get(entry.getKey());
					if (commands == null) {
						tasksWithout.put(entry.getKey(), entry.getValue());
					} else {
						commands.addAll(entry.getValue());
					}
				}
			}
		}

		benchmark.scheduleChanged();

	}

	private void scheaduleList(Map<Long, List<Command>> taskMap, Long currentTime) {

		for (Long shift : taskMap.keySet()) {
			Long time = currentTime + shift;

			List<Command> addedList = taskMap.get(shift);

			synchronized (tasks) {

				List<Command> taskList = tasks.get(time);
				if (taskList == null) {
					taskList = new Vector<Command>();
					tasks.put(time, taskList);
				}

				taskList.addAll(addedList);
			}
		}
	}

	@Override
	public void printWaitingTasks() {

		System.out.println(eventTaskMap.toString());

	}

}
