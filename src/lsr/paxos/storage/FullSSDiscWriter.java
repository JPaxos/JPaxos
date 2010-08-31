package lsr.paxos.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lsr.paxos.Snapshot;

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

public class FullSSDiscWriter implements DiscWriter, PublicDiscWriter {
	private FileOutputStream _logStream;
	private final String _directoryPath;
	private File _directory;
	private DataOutputStream _viewStream;
	private Map<Object, Object> _uselessData = new TreeMap<Object, Object>();
	private Integer _previousSnapshotSeq;
	private Snapshot _snapshot;
	private FileDescriptor _viewStreamFD;

	/* * Record types * */
	/* Sync */
	private static final byte CHANGE_VIEW = 0x01;
	private static final byte CHANGE_VALUE = 0x02;
	private static final byte SNAPSHOT = 0x03;
	private static final byte PAIR = 0x11;
	/* Async */
	private static final byte DECIDED = 0x21;
	private static final byte SEQNO_MARKERS = 0x22;
	

	public FullSSDiscWriter(String directoryPath) throws FileNotFoundException {
		if (directoryPath.endsWith("/"))
			throw new RuntimeException("Directory path cannot ends with /");
		_directoryPath = directoryPath;
		_directory = new File(directoryPath);
		_directory.mkdirs();
		int nextLogNumber = getLastLogNumber(_directory.list()) + 1;
		_logStream = new FileOutputStream(_directoryPath + "/sync." + nextLogNumber + ".log");

		FileOutputStream fos = new FileOutputStream(_directoryPath + "/sync." + nextLogNumber + ".view");

		_viewStream = new DataOutputStream(fos);

		try {
			_viewStreamFD = fos.getFD();
		} catch (IOException e) {
			// Should include better reaction for i-don't-know-what
			throw new RuntimeException("Eeeee... When this happens?");
		}
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
			_logStream.getFD().sync();
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
			_logStream.getFD().sync();

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
			_viewStream.flush();
			_viewStreamFD.sync();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String snapshotFileNameForRequest(int instanceId) {
		return _directoryPath + "/snapshot." + instanceId;
	}

	@Override
	public void newSnapshot(Snapshot snapshot) {
		try {
			assert _previousSnapshotSeq == null || snapshot.requestSeqNo >= _previousSnapshotSeq : "Got order to write OLDER snapshot!!!";

			String filename = snapshotFileNameForRequest(snapshot.requestSeqNo);

			DataOutputStream snapshotStream = new DataOutputStream(new FileOutputStream(filename + "_prep", false));
			snapshot.writeTo(snapshotStream);
			snapshotStream.close();

			if (!new File(filename + "_prep").renameTo(new File(filename)))
				throw new RuntimeException("Not able to record snapshot properly!!!");

			ByteBuffer buffer = ByteBuffer.allocate(1 /* byte type */+ 4 /*
																		 * int
																		 * instance
																		 * ID
																		 */);
			buffer.put(SNAPSHOT);
			buffer.putInt(snapshot.requestSeqNo);

			_logStream.write(buffer.array());

			if (_previousSnapshotSeq != null && _previousSnapshotSeq != snapshot.requestSeqNo)
				new File(snapshotFileNameForRequest(_previousSnapshotSeq)).delete();

			_previousSnapshotSeq = snapshot.requestSeqNo;

			_snapshot = snapshot;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Snapshot getSnapshot() {
		return _snapshot;
	};

	@Override
	public void record(Serializable key, Serializable value) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeObject(key);
			oos.writeObject(value);
			oos.close();

			byte[] ba = baos.toByteArray();

			ByteBuffer bb = ByteBuffer.allocate(1 /* type */
			+ 4 /* length */
			+ ba.length /* data */
			);

			bb.put(PAIR);
			bb.putInt(ba.length);
			bb.put(ba);

			_logStream.write(bb.array());
			_logStream.flush();
			_logStream.getFD().sync();

			_uselessData.put(key, value);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object retrive(Serializable key) {
		return _uselessData.get(key);
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

		if (_previousSnapshotSeq == null)
			return instances.values();

		DataInputStream snapshotStream = new DataInputStream(new FileInputStream(
				snapshotFileNameForRequest(_previousSnapshotSeq)));
		
		_snapshot = new Snapshot(snapshotStream);

		return instances.values();
	}

	private void loadInstances(File file, Map<Integer, ConsensusInstance> instances) throws IOException {
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
					case SEQNO_MARKERS: {
						ConsensusInstance instance = instances.get(id);
						assert instance != null : "SeqNo for non-existing instance";
						int seqNo = stream.readInt();
						int size = stream.readInt();
						BitSet bs = new BitSet(size);
						for (int i = 0; i < size; i++) {
							bs.set(i, stream.readByte() != 0 ? true : false);
						}
						instance.setSeqNoAndMarkers(seqNo, bs);
						break;
					}
					case PAIR: {
						byte[] pair = new byte[id];
						stream.readFully(pair);
						try {
							ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(pair));
							Object key = ois.readObject();
							Object value = ois.readObject();
							_uselessData.put(key, value);

						} catch (ClassNotFoundException e) {
							_logger.log(Level.SEVERE, "Could not find class for a custom log record while recovering");
							e.printStackTrace();
						}
						break;
					}
					case SNAPSHOT: {
						assert _previousSnapshotSeq == null || _previousSnapshotSeq <= id : "Reading an OLDER snapshot ID!!! "
								+ _previousSnapshotSeq + " " + id;
						_previousSnapshotSeq = id;
						break;
					}
					default:
						assert false : "Unrecognized log record type";

				}

			} catch (EOFException e) {
				// it is possible that last chunk of data is corrupted
				_logger.warning("The log file with consensus instaces is incomplete or broken. " + e.getMessage());
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
				_logger.warning("The log file with consensus instaces is incomplete.");
				break;
			}
			lastView = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
		}
		stream.close();

		return lastView;
	}

	private final static Logger _logger = Logger.getLogger(FullSSDiscWriter.class.getCanonicalName());

	@Override
	public void changeInstanceSeqNoAndMarkers(int instanceId, int executeSeqNo, BitSet executeMarker) {
		try {
			ByteBuffer buffer = ByteBuffer.allocate(1 /* byte type */
					+ 4 /* instance ID */ + 4 /* seqNo */+ 4 /* markers length */
					+ executeMarker.length() /* place for markers */);
			buffer.put(SEQNO_MARKERS);
			buffer.putInt(instanceId);
			buffer.putInt(executeSeqNo);
			
			buffer.putInt(executeMarker.length());
			// TODO: keep bit set in bits, not bytes
			for (int i = 0; i < executeMarker.length(); i++) {
				buffer.put((byte) (executeMarker.get(i) ? 1 : 0));
			}
			_logStream.write(buffer.array());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
