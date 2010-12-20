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
    private static final long serialVersionUID = 1L;

    /**
     * The instanceIdArray has ID of undecided instances, finishing with ID from
     * which we have no higher decided
     */
    private int[] instanceIdArray;
    private Range[] instanceIdRanges;
    private boolean snapshotRequest = false;
    private boolean periodicQuery = false;

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
     * @param instanceIdArray - id of unknown instances
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
     * @throws IOException - if I/O error occurs
     */
    public CatchUpQuery(DataInputStream input) throws IOException {
        super(input);
        byte flags = input.readByte();
        periodicQuery = (flags & 1) == 0 ? false : true;
        snapshotRequest = (flags & 2) == 0 ? false : true;

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
     * @param instanceIdArray - list of instances IDs
     */
    public void setInstanceIdList(List<Integer> instanceIdList) {
        instanceIdArray = new int[instanceIdList.size()];
        for (int i = 0; i < instanceIdList.size(); ++i) {
            instanceIdArray[i] = instanceIdList.get(i);
        }
    }

    /**
     * Sets the snapshot request flag. If <code>true</code>,
     * <code>CatchUp</code> mechanism requests for snapshot, otherwise for list
     * of instances.
     * 
     * @param snapshotRequest - <code>true</code> if this is request for
     *            snapshot
     */
    public void setSnapshotRequest(boolean snapshotRequest) {
        this.snapshotRequest = snapshotRequest;
    }

    public void setPeriodicQuery(boolean periodicQuery) {
        this.periodicQuery = periodicQuery;
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
        List<Integer> instanceIdList = new Vector<Integer>();
        for (int i = 0; i < instanceIdArray.length; ++i) {
            instanceIdList.add(instanceIdArray[i]);
        }
        return instanceIdList;
    }

    /**
     * Defines whether this catch up query is for snapshot or for consensus
     * instances.
     * 
     * @return <code>true</code> if this is request for snapshot;
     *         <code>false</code> if this is request for consensus instances
     */
    public boolean isSnapshotRequest() {
        return snapshotRequest;
    }

    public boolean isPeriodicQuery() {
        return periodicQuery;
    }

    public Pair<Integer, Integer>[] getInstanceIdRangeArray() {
        return instanceIdRanges;
    }

    public List<Pair<Integer, Integer>> getInstanceIdRangeList() {
        List<Pair<Integer, Integer>> instanceIdRanges = new Vector<Pair<Integer, Integer>>();
        for (int i = 0; i < this.instanceIdRanges.length; ++i) {
            instanceIdRanges.add(this.instanceIdRanges[i]);
        }
        return instanceIdRanges;
    }

    public MessageType getType() {
        return MessageType.CatchUpQuery;
    }

    public int byteSize() {
        return super.byteSize() + 1 + 4 + instanceIdArray.length * 4 + 4 + instanceIdRanges.length *
               2 * 4;
    }

    public String toString() {
        return (periodicQuery ? "periodic " : "") +
               "CatchUpQuery " +
               (snapshotRequest ? "for snapshot " : "") +
               "(" +
               super.toString() +
               ")" +
               (instanceIdArray != null ? ((" for ranges:" + getInstanceIdRangeList().toString()) +
                                           " and for instances:" + getInstanceIdList().toString())
                       : "");
    }

    protected void write(ByteBuffer bb) {
        bb.put((byte) ((periodicQuery ? 1 : 0) + (snapshotRequest ? 2 : 0)));
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
