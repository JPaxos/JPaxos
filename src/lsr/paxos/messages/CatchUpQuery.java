package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import lsr.common.Pair;
import lsr.common.Range;

/**
 * Represents the catch-up mechanism request message.
 */
public class CatchUpQuery extends Message {
    private static final long serialVersionUID = 1L;

    /**
     * The instanceIdArray has ID of undecided instances, finishing with ID from
     * which we have no higher decided
     */
    private int[] instanceIdArray;
    private Range[] instanceIdRanges;

    /**
     * Creates new <code>CatchUpQuery</code> message.
     * 
     * @param view - the view number
     * @param instanceIdArray - id of unknown instances
     * @param instanceIdRanges - ranges of unknown instances id
     */
    public CatchUpQuery(int view, int[] instanceIdArray, Range[] instanceIdRanges) {
        super(view);
        setInstanceIdRangeArray(instanceIdRanges);
        assert instanceIdArray != null;
        this.instanceIdArray = instanceIdArray;
    }

    /**
     * Creates new <code>CatchUpQuery</code> message.
     * 
     * @param view - the view number
     * @param instanceIdList - id of unknown instances
     * @param instanceIdRanges - ranges of unknown instances id
     */
    public CatchUpQuery(int view, List<Integer> instanceIdList,
                        List<Range> instanceIdRanges) {
        super(view);
        assert instanceIdList != null;
        setInstanceIdList(instanceIdList);
        setInstanceIdRangeList(instanceIdRanges);
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

        instanceIdRanges = new Range[input.readInt()];
        for (int i = 0; i < instanceIdRanges.length; ++i) {
            instanceIdRanges[i] = new Range(input.readInt(), 0);
            instanceIdRanges[i].setValue(input.readInt());
        }

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
     * Sets ranges of requested instances IDs from array. For example, if
     * instances 2, 3 , 4, 8, 9, 10 are requested, then ranges [2, 4] and [8, 9]
     * will be sent in this message.
     * 
     * @param instanceIdRanges - ranges array of requested instances IDs
     */
    public void setInstanceIdRangeArray(Range[] instanceIdRanges) {
        this.instanceIdRanges = instanceIdRanges;
    }

    /**
     * Sets ranges of requested instances IDs from list. For example, if
     * instances 2, 3 , 4, 8, 9, 10 are requested, then ranges [2, 4] and [8, 9]
     * will be sent in this message.
     * 
     * @param instanceIdRanges - ranges list of requested instances IDs
     */
    public void setInstanceIdRangeList(List<Range> instanceIdRanges) {
        this.instanceIdRanges = new Range[instanceIdRanges.size()];
        for (int i = 0; i < instanceIdRanges.size(); ++i) {
            this.instanceIdRanges[i] = instanceIdRanges.get(i);
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
        List<Integer> instanceIdList = new ArrayList<Integer>();
        for (int i = 0; i < instanceIdArray.length; ++i) {
            instanceIdList.add(instanceIdArray[i]);
        }
        return instanceIdList;
    }

    public Pair<Integer, Integer>[] getInstanceIdRangeArray() {
        return instanceIdRanges;
    }

    public List<Range> getInstanceIdRangeList() {
        List<Range> instanceIdRanges = new ArrayList<Range>();
        for (int i = 0; i < this.instanceIdRanges.length; ++i) {
            instanceIdRanges.add(this.instanceIdRanges[i]);
        }
        return instanceIdRanges;
    }

    public MessageType getType() {
        return MessageType.CatchUpQuery;
    }

    public int byteSize() {
        return super.byteSize() + 4 + instanceIdArray.length * 4 + 4 + instanceIdRanges.length *
               2 * 4;
    }

    public String toString() {
        return "CatchUpQuery " +
               "(" +
               super.toString() +
               ")" +
               (instanceIdArray != null ? ((" for ranges:" + getInstanceIdRangeList().toString()) +
                                           " and for instances:" + getInstanceIdList().toString())
                       : "");
    }

    protected void write(ByteBuffer bb) {
        bb.putInt(instanceIdRanges.length);
        for (Pair<Integer, Integer> instance : instanceIdRanges) {
            bb.putInt(instance.key());
            bb.putInt(instance.value());
        }

        bb.putInt(instanceIdArray.length);
        for (int instance : instanceIdArray) {
            bb.putInt(instance);
        }
    }
}
