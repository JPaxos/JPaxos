package lsr.paxos.statistics;

import java.util.Collection;
import java.util.HashMap;

import lsr.paxos.storage.Storage;

public final class QueueMonitor implements Runnable {
    private static QueueMonitor instance = new QueueMonitor();
    public static QueueMonitor getInstance() {
        return instance;
    }


    private final HashMap<String, Collection> queues = new HashMap<String, Collection>();
    private Storage storage;

    private QueueMonitor() {
        new Thread(this).start();
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
        try {
            Thread.sleep(10000);
            StringBuilder sb = new StringBuilder();
            String[] keys = queues.keySet().toArray(new String[]{});
            String sep = "% ";
            for (String tName : keys) {
                sb.append(sep).append(tName);
                sep = "\t";
            }
            sb.append("\talpha\n");
            pLogger.log(sb.toString());
            while (true) {
                Thread.sleep(200);
                int time = (int) (System.currentTimeMillis() - start);
                sb = new StringBuilder();
                sb.append(time + "\t");
                for (String key : keys) {
                    sb.append(queues.get(key).size() + "\t");
                }
                if (storage != null) {
                    sb.append(storage.getWindowUsed());
                }
                sb.append("\n");
                pLogger.log(sb.toString());
            }
        } catch (InterruptedException e) {
        }                
    }
}
