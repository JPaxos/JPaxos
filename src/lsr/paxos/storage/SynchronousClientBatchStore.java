package lsr.paxos.storage;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lsr.common.ClientRequest;
import lsr.paxos.replica.ClientBatchID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class SynchronousClientBatchStore extends ClientBatchStore {

    private static final byte BATCH_VALUE = 0x01;
    private static final byte ASSOCIATE = 0x02;
    private static final byte REMOVE = 0x03;
    private final FileDescriptor fd;
    private final DataOutputStream file;

    protected SynchronousClientBatchStore() {
        super();

        try {
            String logPath = processDescriptor.logPath + '/' + processDescriptor.localId;
            File logDir = new File(logPath);
            logDir.mkdirs();

            Pattern pattern = Pattern.compile("batches\\.(\\d+)\\.log");
            ArrayList<Integer> numbers = new ArrayList<Integer>();
            for (String fileName : logDir.list()) {
                Matcher matcher = pattern.matcher(fileName);
                if (matcher.find()) {
                    int x = Integer.parseInt(matcher.group(1));
                    numbers.add(x);
                }
            }
            Collections.sort(numbers);

            for (Integer number : numbers) {
                DataInputStream dis = new DataInputStream(new FileInputStream(new File(logDir,
                        "batches." + number + ".log")));
                load(dis);
            }

            logger.info("Loaded batch store files");

            int newFileId;
            if (numbers.isEmpty()) {
                newFileId = 0;
            } else {
                newFileId = numbers.get(numbers.size() - 1) + 1;
            }

            FileOutputStream fis = new FileOutputStream(new File(logDir, "batches." + newFileId +
                                                                         ".log"));

            fd = fis.getFD();

            file = new DataOutputStream(fis);

        } catch (Exception e) {
            throw new RuntimeException("Could not read batch values or start new file!", e);
        }
    }

    private void load(DataInputStream dis) throws Exception {
        try {
            while (true) {
                byte type = dis.readByte();
                switch (type) {
                    case BATCH_VALUE: {
                        ClientBatchID cbid = new ClientBatchID(dis);
                        int count = dis.readInt();
                        ClientRequest[] value = new ClientRequest[count];
                        for (int i = 0; i < count; ++i)
                            value[i] = ClientRequest.create(dis);
                        super.setBatch(cbid, value);
                        break;
                    }
                    case REMOVE: {
                        int count = dis.readInt();
                        ArrayList<ClientBatchID> cbids = new ArrayList<ClientBatchID>(count);
                        for (int i = 0; i < count; ++i)
                            cbids.add(new ClientBatchID(dis));
                        super.removeBatches(cbids);
                        break;
                    }
                    case ASSOCIATE: {
                        ClientBatchID cbid = new ClientBatchID(dis);
                        super.associateWithInstance(cbid);
                        break;
                    }
                    default:
                        throw new Exception("Wrong record type");
                }

            }
        } catch (EOFException e) {
        }

    }

    /**
     * Forces synchronizing file with batch values, must be called before any
     * consensus value sync
     */
    public synchronized void sync() throws SyncFailedException {
        fd.sync();
    }

    @Override
    public synchronized void setBatch(ClientBatchID batchId, ClientRequest[] value) {
        super.setBatch(batchId, value);

        try {
            file.writeByte(BATCH_VALUE);
            batchId.writeTo(file);
            file.writeInt(value.length);
            for (int i = 0; i < value.length; i++)
                value[i].writeTo(file);
        } catch (IOException e) {
            fail(e);
        }
    }

    private void fail(IOException e) {
        throw new RuntimeException("Could not write batch value related record", e);
    }

    @Override
    public synchronized void
            associateWithInstance(ClientBatchID batchId) {
        super.associateWithInstance(batchId);

        try {
            file.writeByte(ASSOCIATE);
            batchId.writeTo(file);
        } catch (IOException e) {
            fail(e);
        }
    }

    @Override
    public synchronized void removeBatches(Collection<ClientBatchID> cbids) {
        super.removeBatches(cbids);

        try {
            file.writeByte(REMOVE);
            file.writeInt(cbids.size());
            for (ClientBatchID cbid : cbids)
                cbid.writeTo(file);
        } catch (IOException e) {
            fail(e);
        }
    }

    private final static Logger logger = LoggerFactory.getLogger(SynchronousClientBatchStore.class);
}
