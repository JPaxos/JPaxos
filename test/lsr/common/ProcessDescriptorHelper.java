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
        for (int i = 0; i < numReplicas; i++) {
            processes.add(new PID(i, "localhost", 2000 + i, 3000 + i));
        }
        Configuration configuration = new Configuration(processes);
        ProcessDescriptor.initialize(configuration, localId);
    }
}
