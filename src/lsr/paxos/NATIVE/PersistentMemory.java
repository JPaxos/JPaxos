package lsr.paxos.NATIVE;

import static lsr.common.ProcessDescriptor.processDescriptor;

import java.nio.file.AccessDeniedException;

public final class PersistentMemory {

    private static boolean loaded = false;

    private static native void init(String pmemFile, long pmemFileSize, int numReplicas,
                                    int localId);

    public static native void startThreadLocalTx();

    public static native void commitThreadLocalTx();

    public static void loadLib(String pmemFile) throws UnsatisfiedLinkError, AccessDeniedException {
        if (loaded)
            return;
        System.loadLibrary("jpaxos-pmem");
        init(pmemFile, processDescriptor.nvmPoolSize, processDescriptor.numReplicas,
                processDescriptor.localId);
        loaded = true;
    }

    public static boolean isLoaded() {
        return loaded;
    }
}
