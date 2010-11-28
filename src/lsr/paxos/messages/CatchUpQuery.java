package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;

import lsr.common.Pair;

/**
 * Represents the catch-up mechanism request message
 */
public class CatchUpQuery extends Message {

    private static final long serialVersionUID = 1L;

    /**
     * The _instanceIdArray has ID of undecided instances, finishing with ID
     * from which we have no higher decided
     */
    private int[] instanceIdArray;

    private Pair<Integer, Integer>[] instanceIdRanges;

    private boolean snapshotRequest = false;

    private boolean periodicQuery = false;

    /** Constructors */

    public CatchUpQuery(int view, int[] instanceIdArray, Pair<Integer, Integer>[] instanceIdRanges) {
        super(view);
        setInstanceIdRangeArray(instanceIdRanges);
        assert instanceIdArray != null;
        this.instanceIdArray = instanceIdArray;
    }

    public CatchUpQuery(int view, List<Integer> instanceIdList,
                        List<Pair<Integer, Integer>> instanceIdRanges) {
        super(view);
        assert instanceIdList != null;
        setInstanceIdList(instanceIdList);
        setInstanceIdRangeList(instanceIdRanges);
    }

    @SuppressWarnings("unchecked")
    public CatchUpQuery(DataInputStream input) throws IOException {
        super(input);
        byte flags = input.readByte();
        periodicQuery = (flags & 1) == 0 ? false : true;
        snapshotRequest = (flags & 2) == 0 ? false : true;

        instanceIdRanges = new Pair[input.readInt()];
        for (int i = 0; i < instanceIdRanges.length; ++i) {
            instanceIdRanges[i] = new Pair<Integer, Integer>(input.readInt(), 0);
            instanceIdRanges[i].setValue(input.readInt());
        }

        instanceIdArray = new int[input.readInt()];
        for (int i = 0; i < instanceIdArray.length; ++i) {
            instanceIdArray[i] = input.readInt();
        }
    }

    /** Setters */

    public void setInstanceIdArray(int[] instanceIdArray) {
        this.instanceIdArray = instanceIdArray;
    }

    public void setInstanceIdList(List<Integer> instanceIdList) {
        instanceIdArray = new int[instanceIdList.size()];
        for (int i = 0; i < instanceIdList.size(); ++i) {
            instanceIdArray[i] = instanceIdList.get(i);
        }
    }

    public void setSnapshotRequest(boolean snapshotRequest) {
        this.snapshotRequest = snapshotRequest;
    }

    public void setPeriodicQuery(boolean periodicQuery) {
        this.periodicQuery = periodicQuery;
    }

    public void setInstanceIdRangeArray(Pair<Integer, Integer>[] instanceIdRanges) {
        this.instanceIdRanges = instanceIdRanges;
    }

    @SuppressWarnings("unchecked")
    public void setInstanceIdRangeList(List<Pair<Integer, Integer>> instanceIdRanges) {
        this.instanceIdRanges = new Pair[instanceIdRanges.size()];
        for (int i = 0; i < instanceIdRanges.size(); ++i) {
            this.instanceIdRanges[i] = instanceIdRanges.get(i);
        }
    }

    /** Getters */

    public int[] getInstanceIdArray() {
        return instanceIdArray;
    }

    public List<Integer> getInstanceIdList() {
        List<Integer> instanceIdList = new Vector<Integer>();
        for (int i = 0; i < instanceIdArray.length; ++i) {
            instanceIdList.add(instanceIdArray[i]);
        }
        return instanceIdList;
    }

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

    /** Rest */

    public MessageType getType() {
        return MessageType.CatchUpQuery;
    }

    // protected void write(DataOutputStream os) throws IOException {
    // os.writeByte((_periodicQuery ? 1 : 0) + (_snapshotRequest ? 2 : 0));
    //
    // os.writeInt(_instanceIdRanges.length);
    // for (Pair<Integer, Integer> instance : _instanceIdRanges) {
    // os.writeInt(instance.key());
    // os.writeInt(instance.value());
    // }
    //
    // os.writeInt(_instanceIdArray.length);
    // for (int instance : _instanceIdArray) {
    // os.writeInt(instance);
    // }
    // }

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

    @Override
    protected void write(ByteBuffer bb) throws IOException {
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

    // @Deprecated
    public int byteSize() {
        return super.byteSize() + 1 + 4 + instanceIdArray.length * 4 + 4 + instanceIdRanges.length *
               2 * 4;
    }

}
