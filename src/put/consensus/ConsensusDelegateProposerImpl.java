package put.consensus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import lsr.paxos.client.Client;

public class ConsensusDelegateProposerImpl implements ConsensusDelegateProposer {

    private Client client;

    private BlockingQueue<byte[]> objectsToPropose = new LinkedBlockingQueue<byte[]>();

    protected byte[] byteArrayFromObject(Object object) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new ObjectOutputStream(bos).writeObject(object);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ConsensusDelegateProposerImpl() throws IOException {
        client = new Client();
        client.connect();

        // Starting thread for new proposals
        Thread thread = new Thread() {
            public void run() {
                try {
                    while (true) {
                        client.execute(objectsToPropose.take());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        thread.start();
    }

    public void dispose() {
    }

    public void propose(Object obj) {
        objectsToPropose.add(byteArrayFromObject(obj));
    }

}
