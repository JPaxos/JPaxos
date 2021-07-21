package lsr.paxos.storage;

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

public class PersistentLog implements Log {

    protected NavigableMap<Integer, PersistentConsensusInstance> instanceMapView = new TreeMap<Integer, PersistentConsensusInstance>();
    protected int nextIdCache;

    private static native int[] getExistingInstances();

    public PersistentLog() {
        for (int id : getExistingInstances()) {
            instanceMapView.put(id, new PersistentConsensusInstance(id));
        }
        nextIdCache = getNextId_();
    }

    @Override
    public SortedMap<Integer, PersistentConsensusInstance> getInstanceMap() {
        return Collections.unmodifiableSortedMap(instanceMapView);
    }

    private static native void appendUpTo(int instanceId);

    @Override
    public PersistentConsensusInstance getInstance(int instanceId) {
        boolean resized = (instanceId >= nextIdCache);
        while (instanceId >= nextIdCache) {
            instanceMapView.put(nextIdCache, new PersistentConsensusInstance(nextIdCache));
            nextIdCache++;
        }

        if (resized) {
            appendUpTo(instanceId);
            sizeChanged();
        }
        return instanceMapView.get(instanceId);
    }

    private static native int append_();

    @Override
    public PersistentConsensusInstance append() {
        int id = append_();
        PersistentConsensusInstance ci = new PersistentConsensusInstance(id);
        instanceMapView.put(id, ci);
        nextIdCache = id + 1;
        sizeChanged();
        return ci;
    }

    private static native int getNextId_();

    @Override
    public int getNextId() {
        return nextIdCache;
    }

    @Override
    public native int getLowestAvailableId();

    @Override
    public native long byteSizeBetween(int startId, int endId);

    private static native void truncateBelow_(int instanceId);

    @Override
    public void truncateBelow(int instanceId) {
        truncateBelow_(instanceId);
        /*-
        // // line below confuses JIT and it produces snail-speed code
        //instanceMapView = instanceMapView.tailMap(instanceId, true);
        
        // // lines below are inefficient, as the map will rebalance multiple
        // // times, but java treemap interface is too narrow to do it efficiently
        -*/
        while (!instanceMapView.isEmpty() && instanceMapView.firstKey() < instanceId)
            instanceMapView.pollFirstEntry();
    }

    /// returns the list of removed instances
    private static native int[] clearUndecidedBelow_(int instanceId);

    @Override
    public void clearUndecidedBelow(int instanceId) {
        for (int id : clearUndecidedBelow_(instanceId)) {
            instanceMapView.remove(id);
        }
    }

    /** List of objects to be informed about log changes */
    private List<LogListener> listeners = new Vector<LogListener>();

    @Override
    public boolean addLogListener(LogListener listener) {
        return listeners.add(listener);
    }

    @Override
    public boolean removeLogListener(LogListener listener) {
        return listeners.remove(listener);
    }

    protected void sizeChanged() {
        for (LogListener listener : listeners) {
            listener.logSizeChanged(instanceMapView.size());
        }
    }
}
