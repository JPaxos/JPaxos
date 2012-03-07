package lsr.paxos.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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

public class FullSSDiscWriter implements DiscWriter {
    private FileOutputStream logStream;
    private final String directoryPath;
    private File directory;
    private DataOutputStream viewStream;
    private int snapshotFileNumber = -1;
    private Snapshot snapshot;
    private FileDescriptor viewStreamFD;

    /* * Record types * */
    /* Sync */
    private static final byte CHANGE_VIEW = 0x01;
    private static final byte CHANGE_VALUE = 0x02;
    private static final byte SNAPSHOT = 0x03;
    /* Async */
    private static final byte DECIDED = 0x21;

    public FullSSDiscWriter(String directoryPath) throws FileNotFoundException {
        if (directoryPath.endsWith("/")) {
            throw new RuntimeException("Directory path cannot ends with /");
        }
        this.directoryPath = directoryPath;
        directory = new File(directoryPath);
        directory.mkdirs();
        int nextLogNumber = getLastLogNumber(directory.list()) + 1;
        logStream = new FileOutputStream(this.directoryPath + "/sync." + nextLogNumber + ".log");

        FileOutputStream fos = new FileOutputStream(this.directoryPath + "/sync." + nextLogNumber +
                                                    ".view");

        viewStream = new DataOutputStream(fos);

        try {
            viewStreamFD = fos.getFD();
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
            logStream.write(buffer.array());
            logStream.flush();
            logStream.getFD().sync();
            logger.fine("Log stream sync'd (change instance view)");
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
            (value != null ? value.length : 0));

            buffer.put(CHANGE_VALUE);
            buffer.putInt(instanceId);
            buffer.putInt(view);
            if (value == null) {
                buffer.putInt(-1);
            } else {
                buffer.putInt(value.length);
                buffer.put(value);
            }
            logStream.write(buffer.array());
            logStream.flush();
            logStream.getFD().sync();
            logger.fine("Log stream sync'd (change instance value)");

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
            logStream.write(buffer.array());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void changeViewNumber(int view) {
        try {
            viewStream.writeInt(view);
            viewStream.flush();
            viewStreamFD.sync();
            logger.fine("View stream sync'd");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String snapshotFileName() {
        return directoryPath + "/snapshot." + snapshotFileNumber;
    }

    public void newSnapshot(Snapshot snapshot) {
        try {
            String oldSnapshotFileName = snapshotFileName();
            snapshotFileNumber++;
            String newSnapshotFileName = snapshotFileName();

            DataOutputStream snapshotStream = new DataOutputStream(
                    new FileOutputStream(newSnapshotFileName, false));
            snapshot.writeTo(snapshotStream);
            snapshotStream.close();

            // byte type(1) + int instance id(4)
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4);
            buffer.put(SNAPSHOT);
            buffer.putInt(snapshotFileNumber);
            logStream.write(buffer.array());

            if (new File(oldSnapshotFileName).exists()) {
                if (!new File(oldSnapshotFileName).delete()) {
                    throw new RuntimeException("File removal failed!");
                }
            }

            this.snapshot = snapshot;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Snapshot getSnapshot() {
        return snapshot;
    };

    public void close() throws IOException {
        logStream.close();
        viewStream.close();
    }

    public Collection<ConsensusInstance> load() throws IOException {
        Pattern pattern = Pattern.compile("sync\\.(\\d+)\\.log");
        List<Integer> numbers = new ArrayList<Integer>();
        for (String fileName : directory.list()) {
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
            loadInstances(new File(directoryPath + "/" + fileName), instances);
        }

        if (snapshotFileNumber == -1) {
            return instances.values();
        }

        DataInputStream snapshotStream = new DataInputStream(
                new FileInputStream(snapshotFileName()));

        snapshot = new Snapshot(snapshotStream);
        snapshotStream.close();

        return instances.values();
    }

    private void loadInstances(File file, Map<Integer, ConsensusInstance> instances)
            throws IOException {
        DataInputStream stream = new DataInputStream(new FileInputStream(file));

        while (true) {
            try {
                int type = stream.read();
                if (type == -1) {
                    break;
                }
                int id = stream.readInt();

                switch (type) {
                    case CHANGE_VIEW: {
                        int view = stream.readInt();
                        if (instances.get(id) == null) {
                            instances.put(id, new ConsensusInstance(id));
                        }
                        ConsensusInstance instance = instances.get(id);
                        instance.setView(view);
                        break;
                    }
                    case CHANGE_VALUE: {
                        int view = stream.readInt();
                        int length = stream.readInt();
                        byte[] value;
                        if (length == -1) {
                            value = null;
                        } else {
                            value = new byte[length];
                            stream.readFully(value);
                        }
                        if (instances.get(id) == null) {
                            instances.put(id, new ConsensusInstance(id));
                        }
                        ConsensusInstance instance = instances.get(id);
                        instance.updateStateFromKnown(view, value);
                        break;
                    }
                    case DECIDED: {
                        ConsensusInstance instance = instances.get(id);
                        assert instance != null : "Decide for non-existing instance";
                        instance.setDecided();
                        break;
                    }
                    case SNAPSHOT: {
                        snapshotFileNumber = id;
                        break;
                    }
                    default:
                        assert false : "Unrecognized log record type";

                }

            } catch (EOFException e) {
                // it is possible that last chunk of data is corrupted
                logger.warning("The log file with consensus instaces is incomplete or broken. " +
                               e.getMessage());
                break;
            }
        }
        stream.close();
    }

    public int loadViewNumber() throws IOException {
        Pattern pattern = Pattern.compile("sync\\.(\\d+)\\.view");
        List<String> files = new ArrayList<String>();
        for (String fileName : directory.list()) {
            if (pattern.matcher(fileName).matches()) {
                files.add(fileName);
            }
        }
        int lastView = 0;

        for (String fileName : files) {
            int x = loadLastViewNumber(new File(directoryPath + "/" + fileName));
            if (lastView < x) {
                lastView = x;
            }
        }
        return lastView;
    }

    private int loadLastViewNumber(File file) throws IOException {
        DataInputStream stream = new DataInputStream(new FileInputStream(file));
        int lastView = 0;
        while (true) {
            int ch1 = stream.read();
            if (ch1 < 0) {
                break;
            }
            int ch2 = stream.read();
            int ch3 = stream.read();
            int ch4 = stream.read();
            if ((ch1 | ch2 | ch3 | ch4) < 0) {
                logger.warning("The log file with consensus instaces is incomplete.");
                break;
            }
            lastView = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
        }
        stream.close();

        return lastView;
    }

    private final static Logger logger = Logger.getLogger(FullSSDiscWriter.class.getCanonicalName());
}
