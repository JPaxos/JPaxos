package lsr.paxos.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import lsr.service.AbstractService;

@Deprecated
public class SimpleService extends AbstractService {
    private int sum = 0;

    /*
     * (non-Javadoc)
     * 
     * @see lsr.service.Service#execute(lsr.common.Request)
     */
    public byte[] execute(byte[] value, int seqNo) {
        ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(value);
        DataInputStream dataInput = new DataInputStream(byteArrayInput);

        int toAdd;
        try {
            toAdd = dataInput.readInt();
        } catch (IOException e) {
            return null;
        }
        sum += toAdd;

        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(byteArrayOutput);
        try {
            dataOutput.writeInt(sum);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return byteArrayOutput.toByteArray();
    }

    public void askForSnapshot(int lastSnapshotInstance) {

    }

    public void forceSnapshot(int lastSnapshotInstance) {

    }

    public void updateToSnapshot(int instanceId, byte[] snapshot) {

    }

    public void instanceExecuted(int instanceId) {

    }
}
