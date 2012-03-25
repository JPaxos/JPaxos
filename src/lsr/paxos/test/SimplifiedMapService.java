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

import lsr.service.SimplifiedService;

public class SimplifiedMapService extends SimplifiedService {
    private HashMap<Long, Long> map = new HashMap<Long, Long>();

    protected byte[] execute(byte[] value) {
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

        map.put(command.getKey(), command.getValue());

        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(byteArrayOutput);
        try {
            dataOutput.writeLong(x);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return byteArrayOutput.toByteArray();
    }

    protected byte[] makeSnapshot() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(stream);
            objectOutputStream.writeObject(map);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return stream.toByteArray();
    }

    @SuppressWarnings("unchecked")
    protected void updateToSnapshot(byte[] snapshot) {
        ByteArrayInputStream stream = new ByteArrayInputStream(snapshot);
        ObjectInputStream objectInputStream;
        try {
            objectInputStream = new ObjectInputStream(stream);
            map = (HashMap<Long, Long>) objectInputStream.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static final Logger logger = Logger.getLogger(SimplifiedMapService.class.getCanonicalName());
}
