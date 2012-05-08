package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;

import lsr.common.Pair;
import lsr.common.Range;

/**
 * Represents the catch-up mechanism request message.
 */
public class RecoveryQuery extends Message {

    /**
     * The instanceIdArray has ID of undecided instances, finishing with ID from
     * which we have no higher decided
     */
    private int[] instanceIdArray;
    private int catchUpId;

    public RecoveryQuery(int view, int[] instanceIdArray, int catchUpId) {
        super(view);
        assert instanceIdArray != null;
		this.catchUpId = catchUpId;
        this.instanceIdArray = instanceIdArray;
    }

    public RecoveryQuery(int view, List<Integer> instanceIdList, int catchUpId) {
        super(view);
        assert instanceIdList != null;
		this.catchUpId = catchUpId;
        setInstanceIdList(instanceIdList);
    }

    public RecoveryQuery(DataInputStream input) throws IOException {
        super(input);
        byte flags = input.readByte();

        instanceIdArray = new int[input.readInt()];
        for (int i = 0; i < instanceIdArray.length; ++i) {
            instanceIdArray[i] = input.readInt();
        }
		
		catchUpId = input.readInt();
	}

    /**
     * Sets requested instances IDs from array.
     * 
     * @param instanceIdArray - array of instances IDs
     */
    public void setInstanceIdArray(int[] instanceIdArray) {
        this.instanceIdArray = instanceIdArray;
    }

    /**
     * Sets requested instances IDs from list.
     * 
     * @param instanceIdList - list of instances IDs
     */
    public void setInstanceIdList(List<Integer> instanceIdList) {
        instanceIdArray = new int[instanceIdList.size()];
        for (int i = 0; i < instanceIdList.size(); ++i) {
            instanceIdArray[i] = instanceIdList.get(i);
        }
    }

    /**
     * Returns array of requested instances IDs (unknown for sender).
     * 
     * @return array of requested instances IDs
     */
    public int[] getInstanceIdArray() {
        return instanceIdArray;
    }

    /**
     * Returns list of requested instances IDs (unknown for sender).
     * 
     * @return list of requested instances IDs
     */
    public List<Integer> getInstanceIdList() {
        List<Integer> instanceIdList = new Vector<Integer>();
        for (int i = 0; i < instanceIdArray.length; ++i) {
            instanceIdList.add(instanceIdArray[i]);
        }
        return instanceIdList;
    }

    public MessageType getType() {
        return MessageType.RecoveryQuery;
    }
	
	public int getCatchUpId() {
        return catchUpId;
    }

    public int byteSize() {
        return super.byteSize() + 4 + 4 + instanceIdArray.length * 4;
    }

    public String toString() {
        return "RecoveryQuery " +"(" + super.toString() + ") ( catchUpId : " + catchUpId + ")" +
               (instanceIdArray != null ? (" for instances:" + getInstanceIdList().toString()): "");
    }

    protected void write(ByteBuffer bb) {

        bb.putInt(instanceIdArray.length);
        for (int instance : instanceIdArray) {
            bb.putInt(instance);
        }
        bb.putInt(catchUpId);
    }
}
