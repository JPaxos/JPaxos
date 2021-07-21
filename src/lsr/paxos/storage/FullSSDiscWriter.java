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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsr.common.CrashModel;
import lsr.common.ProcessDescriptor;
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

    private final String directoryPath;
    private File directory;

    private int currentLogStreamNumber;
    private int highestInstanceInCurrentStream = Integer.MIN_VALUE;
    private FileOutputStream logStream;
    // this lock protects logStream from changing while it's written to
    private ReentrantReadWriteLock logStreamMutex = new ReentrantReadWriteLock();

    private DataOutputStream viewStream;
    private FileDescriptor viewStreamFD;

    private final SynchronousClientBatchStore batchStore;

    private int snapshotFileNumber = -1;
    private Snapshot snapshot;
    private TreeMap<Integer, Integer> logStreamHighestInstance = new TreeMap<>();

    /* * Record types * */
    /* Sync */
    // view has a new stream to work more robust upon with recovery
    // private static final byte CHANGE_VIEW = 0x01;
    private static final byte CHANGE_VALUE = 0x02;
    private static final byte SNAPSHOT = 0x03;
    /* Async */
    private static final byte DECIDED = 0x21;

    public FullSSDiscWriter(String directoryPath) throws FileNotFoundException {
        assert CrashModel.FullSS.equals(ProcessDescriptor.processDescriptor.crashModel);
        if (directoryPath.endsWith("/")) {
            throw new RuntimeException("Directory path cannot end with /");
        }
        this.directoryPath = directoryPath;
        directory = new File(directoryPath);
        directory.mkdirs();
        currentLogStreamNumber = getLastLogNumber() + 1;
        logStream = new FileOutputStream(
                this.directoryPath + "/sync." + currentLogStreamNumber + ".log");

        FileOutputStream fos = new FileOutputStream(
                this.directoryPath + "/sync." + currentLogStreamNumber + ".view");

        viewStream = new DataOutputStream(fos);

        try {
            viewStreamFD = fos.getFD();
        } catch (IOException e) {
            throw new RuntimeException("Eeeee... When this happens?", e);
        }

        if (ProcessDescriptor.processDescriptor.indirectConsensus)
            batchStore = (SynchronousClientBatchStore) ClientBatchStore.instance;
        else
            batchStore = null;
    }

    private ArrayList<Integer> logStreamNumbersInFilesystem() {
        Pattern pattern = Pattern.compile("sync\\.(\\d+)\\.log");
        ArrayList<Integer> numbers = new ArrayList<Integer>();
        for (String fileName : directory.list()) {
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.find()) {
                int x = Integer.parseInt(matcher.group(1));
                numbers.add(x);
            }
        }
        Collections.sort(numbers);
        return numbers;
    }

    private int getLastLogNumber() {
        ArrayList<Integer> nums = logStreamNumbersInFilesystem();
        if (nums.isEmpty())
            return -1;
        return nums.get(nums.size() - 1);
    }

    private ByteBuffer changeInstanceValueBuffer = ByteBuffer.allocate(
            1 + /*- byte type -*/4 + /*- int instanceID -*/ 4 + /*- int view -*/ 4 /*- int length of value -*/
    );
    {
        changeInstanceValueBuffer.put(CHANGE_VALUE);
    }

    public void changeInstanceValue(int instanceId, int view, byte[] value) {
        try {
            if (instanceId > highestInstanceInCurrentStream)
                highestInstanceInCurrentStream = instanceId;

            changeInstanceValueBuffer.position(1);
            changeInstanceValueBuffer.putInt(instanceId);
            changeInstanceValueBuffer.putInt(view);
            changeInstanceValueBuffer.putInt(value == null ? -1 : value.length);

            Lock lock = logStreamMutex.readLock();
            lock.lock();
            try {
                logStream.write(changeInstanceValueBuffer.array());
                if (value != null)
                    logStream.write(value);

                if (batchStore != null)
                    batchStore.sync();
                logStream.flush();
                logStream.getFD().sync();
            } finally {
                lock.unlock();
            }

            logger.debug("Log stream sync'd (change instance value)");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    ByteBuffer decideInstanceBuffer = ByteBuffer.allocate(
            1 + /*- byte type -*/ 4 /*- int instance ID -*/);
    {
        decideInstanceBuffer.put(DECIDED);
    }

    public void decideInstance(int instanceId) {
        try {
            if (instanceId > highestInstanceInCurrentStream)
                highestInstanceInCurrentStream = instanceId;

            decideInstanceBuffer.position(1);
            decideInstanceBuffer.putInt(instanceId);
            Lock lock = logStreamMutex.readLock();
            lock.lock();
            try {
                logStream.write(decideInstanceBuffer.array());
            } finally {
                lock.unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void changeViewNumber(int view) {
        try {
            viewStream.writeInt(view);

            viewStream.flush();
            viewStreamFD.sync();

            logger.debug("View stream sync'd");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String snapshotFileName() {
        return directoryPath + "/snapshot." + snapshotFileNumber;
    }

    // byte type(1) + int instance id(4)
    ByteBuffer newSnapshotBuffer = ByteBuffer.allocate(1 + 4);
    {
        newSnapshotBuffer.put(SNAPSHOT);
    }

    public void newSnapshot(Snapshot snapshot) {
        try {
            String oldSnapshotFileName = snapshotFileName();
            snapshotFileNumber++;
            String newSnapshotFileName = snapshotFileName();

            FileOutputStream fos = new FileOutputStream(newSnapshotFileName, false);
            DataOutputStream snapshotStream = new DataOutputStream(fos);
            snapshot.writeTo(snapshotStream);
            fos.getFD().sync();
            snapshotStream.close();

            newSnapshotBuffer.position(1);
            newSnapshotBuffer.putInt(snapshotFileNumber);

            Lock lock = logStreamMutex.writeLock();
            // lock is unlocked in rotateLogStream
            lock.lock();

            logStream.write(newSnapshotBuffer.array());

            rotateLogStream(snapshot.getNextInstanceId(), lock);

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

    // Removes unnecessary log records
    private void rotateLogStream(Integer snapshotNextInst, Lock lock) throws IOException {
        logStreamHighestInstance.put(currentLogStreamNumber, highestInstanceInCurrentStream);
        highestInstanceInCurrentStream = Integer.MIN_VALUE;

        try {
            logStream.close();

            currentLogStreamNumber++;

            logStream = new FileOutputStream(
                    this.directoryPath + "/sync." + currentLogStreamNumber + ".log");
        } finally {
            lock.unlock();
        }

        List<Integer> logStreamFiles = logStreamNumbersInFilesystem();

        for (int logStreamNb : logStreamFiles) {
            if (logStreamNb >= currentLogStreamNumber - 1)
                // previous log stream contains information about the snapshot,
                // so it must survive
                break;
            if (logStreamHighestInstance.containsKey(logStreamNb)) {
                if (logStreamHighestInstance.get(logStreamNb) >= snapshotNextInst)
                    break;
                logStreamHighestInstance.remove(logStreamNb);
                logger.debug(
                        "Pruning SyncLog file {} that has instances up to {} (snapshot nextInst is {})",
                        this.directoryPath + "/sync." + logStreamNb + ".log",
                        logStreamHighestInstance.get(logStreamNb), snapshotNextInst);
            } else {
                logger.warn("Pruning unknown SyncLog file {}",
                        this.directoryPath + "/sync." + logStreamNb + ".log");
            }
            String fn = this.directoryPath + "/sync." + logStreamNb + ".log";
            File redundant = new File(fn);
            if (!redundant.delete())
                throw new RuntimeException("File removal failed!");
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
        List<Integer> numbers = logStreamNumbersInFilesystem();

        Map<Integer, ConsensusInstance> instances = new TreeMap<Integer, ConsensusInstance>();
        for (Integer number : numbers) {
            loadInstances(number, instances);
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

    private void loadInstances(int number, Map<Integer, ConsensusInstance> instances)
            throws IOException {
        File file = new File(directoryPath + "/" + ("sync." + number + ".log"));
        DataInputStream stream = new DataInputStream(new FileInputStream(file));

        int maxInst = Integer.MIN_VALUE;

        while (true) {
            try {
                int type = stream.read();
                if (type == -1) {
                    break;
                }
                int id = stream.readInt();

                switch (type) {
                    case CHANGE_VALUE: {
                        if (maxInst < id)
                            maxInst = id;

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
                            instances.put(id, new InMemoryConsensusInstance(id));
                        }
                        ConsensusInstance instance = instances.get(id);
                        instance.updateStateFromPropose(ProcessDescriptor.processDescriptor.localId,
                                view, value);
                        break;
                    }
                    case DECIDED: {
                        if (maxInst < id)
                            maxInst = id;

                        ConsensusInstance instance = instances.get(id);
                        if (instance != null)
                            instance.setDecided();
                        break;
                    }
                    case SNAPSHOT: {
                        snapshotFileNumber = id;
                        break;
                    }
                    default:
                        throw new AssertionError("Unrecognized log record type");

                }

            } catch (EOFException e) {
                // it is possible that last chunk of data is corrupted
                logger.warn("The log file with consensus instaces is incomplete or broken: {}  {}",
                        file, e.getMessage());
                break;
            }
        }
        stream.close();

        logStreamHighestInstance.put(number, maxInst);
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

        // not reading the last file only, as no view change may have happened
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
            /*
             * (JK) This purely evil code checks if the crash did not happen
             * while writing the view to disk. Bare stream.readInt() would not
             * allow this. I'm leaving the code as is, because it just works.
             */
            int ch1 = stream.read();
            if (ch1 < 0) {
                break;
            }
            int ch2 = stream.read();
            int ch3 = stream.read();
            int ch4 = stream.read();
            if ((ch2 | ch3 | ch4) < 0) {
                logger.warn("The log file with view numbers is incomplete: {}", file);
                break;
            }
            lastView = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
        }
        stream.close();

        return lastView;
    }

    private final static Logger logger = LoggerFactory.getLogger(FullSSDiscWriter.class);

}
