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
public class CatchUpQuery extends Message {
    private static final long serialVersionUID = 1L; // LISA What is that for?

    /**
     * The instanceIdArray has ID of undecided instances, finishing with ID from
     * which we have no higher decided
     */
    private int[] instanceIdArray;

    /**
     * Creates new <code>CatchUpQuery</code> message.
     * 
     * @param view - the view number
     * @param instanceIdArray - id of unknown instances
     */
    public CatchUpQuery(int view, int[] instanceIdArray) {
        super(view);
        assert instanceIdArray != null;
        this.instanceIdArray = instanceIdArray;
    }

    /**
     * Creates new <code>CatchUpQuery</code> message.
     * 
     * @param view - the view number
     * @param instanceIdList - id of unknown instances
     */
    public CatchUpQuery(int view, List<Integer> instanceIdList) {
        super(view);
        assert instanceIdList != null;
        setInstanceIdList(instanceIdList);
    }

    /**
     * Creates new <code>CatchUpQuery</code> message from input stream with
     * serialized message.
     * 
     * @param input - the input stream with serialized message
     * @throws IOException if I/O error occurs
     */
    public CatchUpQuery(DataInputStream input) throws IOException {
        super(input);
        byte flags = input.readByte();

        instanceIdArray = new int[input.readInt()];
        for (int i = 0; i < instanceIdArray.length; ++i) {
            instanceIdArray[i] = input.readInt();
        }
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
        return MessageType.CatchUpQuery;
    }

    public int byteSize() {
        return super.byteSize() + 1 + 4 + instanceIdArray.length * 4 + 4;
    }

    public String toString() {
        return "CatchUpQuery " +"(" + super.toString() + ")" +
               (instanceIdArray != null ? (" for instances:" + getInstanceIdList().toString()): "");
    }

    protected void write(ByteBuffer bb) {

        bb.putInt(instanceIdArray.length);
        for (int instance : instanceIdArray) {
            bb.putInt(instance);
        }
    }
}
