package lsr.paxos.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import lsr.paxos.storage.Storage;

public final class QueueMonitor implements Runnable {
    private static QueueMonitor instance = new QueueMonitor();
    public static QueueMonitor getInstance() {
        return instance;
    }

    
    private final HashMap<String, Collection> queues = new HashMap<String, Collection>();
    private Storage storage;

    private QueueMonitor() {
        // new Thread(new Monitor()).start();
    }

    public void registerQueue(String name, Collection queue) {
        queues.put(name, queue);
    }

    public void registerLog(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();            
        PerformanceLogger pLogger = PerformanceLogger.getLogger("queues");
        while (true) {
            try {
                Thread.sleep(200);
                int time = (int) (System.currentTimeMillis() - start);
                StringBuilder sb = new StringBuilder();
                sb.append(time + "\t");
                for (Entry<String, Collection>  entry : queues.entrySet()) {
                    sb.append(entry.getKey() + ":" + entry.getValue().size() + "\t");
                }
                if (storage != null) {
                    sb.append("alpha:" + storage.getWindowUsed());
                }
                sb.append("\n");
                pLogger.log(sb.toString());
            } catch (InterruptedException e) {
            }                
        }
    }
}
