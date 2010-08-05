package lsr.paxos.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of an incremental log - each event is recorded as a byte
 * pattern.
 * 
 * First byte determines type of log record, latter usually contain instance ID,
 * view and value.
 * 
 * The writer always start a new file in provided directory.
 * 
 * @author Tomasz Żurkowski
 * @author Jan Kończak
 */

public class FullSSDiscWriter implements DiscWriter {
	private OutputStream _logStream;
	private final String _directoryPath;
	private File _directory;
	private DataOutputStream _viewStream;

	/*  * Record types * */
	/* Sync */
	private static final byte CHANGE_VIEW = 0x01;
	private static final byte CHANGE_VALUE = 0x02;
	/* Async */
	private static final byte DECIDED = 0x21;

	public FullSSDiscWriter(String directoryPath) throws FileNotFoundException {
		if (directoryPath.endsWith("/"))
			throw new RuntimeException("Directory path cannot ends with /");
		_directoryPath = directoryPath;
		_directory = new File(directoryPath);
		_directory.mkdirs();
		int nextLogNumber = getLastLogNumber(_directory.list()) + 1;
		_logStream = new FileOutputStream(_directoryPath + "/sync."
				+ nextLogNumber + ".log");
		_viewStream = new DataOutputStream(new FileOutputStream(_directoryPath
				+ "/sync." + nextLogNumber + ".view"));
	}

	protected int getLastLogNumber(String[] files) {
		Pattern pattern = Pattern.compile("sync\\.(\\d+)\\.log");
		int last = -1;
		for (String fileName : files) {
			Matcher matcher = pattern.matcher(fileName);
			if (matcher.find()) {
				int x = Integer.parseInt(matcher.group(1));
				last = Math.max(x, last);
			}
		}
		return last;
	}

	public void changeInstanceView(int instanceId, int view) {
		try {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4);
			buffer.put(CHANGE_VIEW);
			buffer.putInt(instanceId);
			buffer.putInt(view);
			_logStream.write(buffer.array());
			_logStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void changeInstanceValue(int instanceId, int view, byte[] value) {
		try {

			ByteBuffer buffer = ByteBuffer.allocate(1 + /* byte type */
			4 + /* int instance ID */
			4 + /* int view */
			4 + /* int length of value */
			value.length);
			buffer.put(CHANGE_VALUE);
			buffer.putInt(instanceId);
			buffer.putInt(view);
			buffer.putInt(value.length);
			buffer.put(value);
			_logStream.write(buffer.array());
			_logStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void decideInstance(int instanceId) {
		try {
			ByteBuffer buffer = ByteBuffer.allocate(1 + /* byte type */
			4/* int instance ID */);
			buffer.put(DECIDED);
			buffer.putInt(instanceId);
			_logStream.write(buffer.array());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void changeViewNumber(int view) {
		try {
			_viewStream.writeInt(view);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() throws IOException {
		_logStream.close();
		_viewStream.close();
	}

	public Collection<ConsensusInstance> load() throws IOException {
		Pattern pattern = Pattern.compile("sync\\.(\\d+)\\.log");
		List<Integer> numbers = new ArrayList<Integer>();
		for (String fileName : _directory.list()) {
			Matcher matcher = pattern.matcher(fileName);
			if (matcher.find()) {
				int x = Integer.parseInt(matcher.group(1));
				numbers.add(x);
			}
		}
		Collections.sort(numbers);

		Map<Integer, ConsensusInstance> instances = new TreeMap<Integer, ConsensusInstance>();
		for (Integer number : numbers) {
			String fileName = "sync." + number + ".log";
			loadInstances(new File(_directoryPath + "/" + fileName), instances);
		}
		return instances.values();
	}

	private static void loadInstances(File file,
			Map<Integer, ConsensusInstance> instances) throws IOException {
		DataInputStream stream = new DataInputStream(new FileInputStream(file));

		while (true) {
			try {
				int type = stream.read();
				if (type == -1)
					break;
				int id = stream.readInt();

				switch (type) {
				case CHANGE_VIEW: {
					int view = stream.readInt();
					if (instances.get(id) == null)
						instances.put(id, new ConsensusInstance(id));
					ConsensusInstance instance = instances.get(id);
					instance.setView(view);
					break;
				}
				case CHANGE_VALUE: {
					int view = stream.readInt();
					int length = stream.readInt();
					byte[] value = new byte[length];
					stream.readFully(value);

					if (instances.get(id) == null)
						instances.put(id, new ConsensusInstance(id));
					ConsensusInstance instance = instances.get(id);
					instance.setValue(view, value);
					break;
				}
				case DECIDED: {
					ConsensusInstance instance = instances.get(id);
					assert instance != null : "Decide for non-existing instance";
					instance.setDecided();
					break;
				}
				default:
					assert false : "Unrecognized log record type";

				}

			} catch (EOFException e) {
				// it is possible that last chunk of data is corrupted
				_logger
						.warning("The log file with consensus instaces is incomplete or broken. "
								+ e.getMessage());
				break;
			}
		}
		stream.close();
	}

	public int loadViewNumber() throws IOException {
		Pattern pattern = Pattern.compile("sync\\.(\\d+)\\.view");
		List<String> files = new ArrayList<String>();
		for (String fileName : _directory.list()) {
			if (pattern.matcher(fileName).matches()) {
				files.add(fileName);
			}
		}
		int lastView = 0;

		for (String fileName : files) {
			int x = loadLastViewNumber(new File(_directoryPath + "/" + fileName));
			if (lastView < x)
				lastView = x;
		}
		return lastView;
	}

	private int loadLastViewNumber(File file) throws IOException {
		DataInputStream stream = new DataInputStream(new FileInputStream(file));
		int lastView = 0;
		while (true) {
			int ch1 = stream.read();
			if (ch1 < 0)
				break;
			int ch2 = stream.read();
			int ch3 = stream.read();
			int ch4 = stream.read();
			if ((ch1 | ch2 | ch3 | ch4) < 0) {
				_logger
						.warning("The log file with consensus instaces is incomplete.");
				break;
			}
			lastView = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
		}
		stream.close();

		return lastView;
	}

	private final static Logger _logger = Logger
			.getLogger(FullSSDiscWriter.class.getCanonicalName());

}
