package lsr.paxos.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Collection;

import lsr.paxos.Snapshot;

//TEST class to see which solution is faster Nio or standard streams

public class NioFullSSDiscWriter implements DiscWriter {
	FileChannel channel;

	public NioFullSSDiscWriter(String directoryPath)

	throws FileNotFoundException {
		throw new RuntimeException("Not implemented!");

		// channel = new FileOutputStream(directoryPath +
		// "/nio.sync.log").getChannel();
	}

	public void changeInstanceValue(int instanceId, int view, byte[] value) {
		try {
			ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + 4 + value.length);
			buffer.put((byte) 2);
			buffer.putInt(instanceId);
			buffer.putInt(view);
			buffer.putInt(value.length);
			buffer.put(value);
			channel.write(buffer);
			channel.force(false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void changeInstanceView(int instanceId, int view) {
		// TODO Auto-generated method stub

	}

	public void changeViewNumber(int view) {
		// TODO Auto-generated method stub

	}

	public void decideInstance(int instanceId) {
		// TODO Auto-generated method stub

	}

	public void close() throws IOException {
		channel.close();
	}

	public Collection<ConsensusInstance> load() throws IOException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	public int loadViewNumber() throws IOException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public void newSnapshot(Snapshot snapshot) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Unimplemented!");

	}

	@Override
	public Snapshot getSnapshot() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Unimplemented!");
	}

	@Override
	public void changeInstanceSeqNoAndMarkers(int instanceId, int executeSeqNo, BitSet executeMarker) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Unimplemented!");
	}

}
