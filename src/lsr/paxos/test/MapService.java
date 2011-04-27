package lsr.paxos.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.service.AbstractService;

public class MapService extends AbstractService {
    private HashMap<Long, Long> map = new HashMap<Long, Long>();
    private int lastSeq = 0;

    public byte[] execute(byte[] value, int seqNo) {
        lastSeq = seqNo;
        MapServiceCommand command;
        try {
            command = new MapServiceCommand(value);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Incorrect request", e);
            return null;
        }

        Long x = map.get(command.getKey());
        if (x == null) {
            x = Long.valueOf(0);
        }

        x = command.getA() * x + command.getB();
        map.put(command.getKey(), x);

        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(byteArrayOutput);
        try {
            dataOutput.writeLong(x);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return byteArrayOutput.toByteArray();
    }

    public int hashCode() {
        return map.hashCode();
    }

    public void askForSnapshot(int lastSnapshotInstance) {
        forceSnapshot(lastSnapshotInstance);
    }

    public void forceSnapshot(int lastSnapshotInstance) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(stream);
            objectOutputStream.writeObject(map);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        fireSnapshotMade(lastSeq + 1, stream.toByteArray(), null);
    }

    @SuppressWarnings("unchecked")
    public void updateToSnapshot(int nextSeq, byte[] snapshot) {
        lastSeq = nextSeq - 1;
        ByteArrayInputStream stream = new ByteArrayInputStream(snapshot);
        ObjectInputStream objectInputStream;
        try {
            objectInputStream = new ObjectInputStream(stream);
            map = (HashMap<Long, Long>) objectInputStream.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void instanceExecuted(int instanceId) {
        // ignoring
    }

    private static final Logger logger = Logger.getLogger(MapService.class.getCanonicalName());
}
