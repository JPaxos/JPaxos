package lsr.common;

import java.util.ArrayList;
import java.util.List;

public class ProcessDescriptorHelper {
    /**
     * Initialize <code>ProcessDescriptor</code> singleton with specified number
     * of replicas and local ID.
     * 
     * @param numReplicas - the number of replicas to initialize
     * @param localId - the id of local process
     */
    public static void initialize(int numReplicas, int localId) {
        List<PID> processes = new ArrayList<PID>();
        processes.add(new PID(0, "localhost", 2000, 3000));
        processes.add(new PID(1, "localhost", 2001, 3001));
        processes.add(new PID(2, "localhost", 2002, 3002));
        Configuration configuration = new Configuration(processes);
        ProcessDescriptor.initialize(configuration, localId);
    }
}
