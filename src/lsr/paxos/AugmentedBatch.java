package lsr.paxos;

import java.nio.ByteBuffer;

import lsr.common.ClientRequest;
import lsr.common.RequestType;

public class AugmentedBatch {

    public static class BatchId {
        int leader;
        int reignPeriod;
        long seqNum;

        BatchId(int leader, int reignPeriod, long seqNum) {
            this.leader = leader;
            this.reignPeriod = reignPeriod;
            this.seqNum = seqNum;
        }

        BatchId(ByteBuffer bb) {
            leader = bb.getInt();
            reignPeriod = bb.getInt();
            seqNum = bb.getLong();
        }

        void writeTo(ByteBuffer bb) {
            bb.putInt(leader);
            bb.putInt(reignPeriod);
            bb.putLong(seqNum);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof BatchId))
                return false;
            BatchId other = (BatchId) o;
            return this.leader == other.leader && this.reignPeriod == other.reignPeriod &&
                   this.seqNum == other.seqNum;
        }
        
        static BatchId startBatch = new BatchId(-1, -1, -1); 

        static int byteSize() {
            return 16;
        }
    }

    private BatchId previousBatchId;
    private BatchId batchId;
    private ClientRequest[] requests;

    public AugmentedBatch(BatchId previousBatchId, BatchId batchId, ClientRequest[] requests) {
        this.previousBatchId = previousBatchId;
        this.batchId = batchId;
        this.requests = requests;
    }

    public AugmentedBatch(byte[] source) {
        ByteBuffer bb = ByteBuffer.wrap(source);

        previousBatchId = new BatchId(bb);
        batchId = new BatchId(bb);

        int count = bb.getInt();
        requests = new ClientRequest[count];

        for (int i = 0; i < count; ++i) {
            requests[i] = ClientRequest.create(bb);
        }

        assert bb.remaining() == 0 : "Packing/unpacking error";
    }

    public BatchId getPreviousBatchId() {
        return previousBatchId;
    }

    public BatchId getBatchId() {
        return batchId;
    }

    public ClientRequest[] getRequests() {
        return requests;
    }

    public byte[] toByteArray()
    {
        int batchSize = BatchId.byteSize() * 2 + 4;
        for (ClientRequest req : requests)
            batchSize += req.byteSize();

        ByteBuffer bb = ByteBuffer.allocate(batchSize);

        previousBatchId.writeTo(bb);
        batchId.writeTo(bb);

        bb.putInt(requests.length);
        for (RequestType req : requests) {
            req.writeTo(bb);
        }
        byte[] value = bb.array();

        return value;
    }
}
