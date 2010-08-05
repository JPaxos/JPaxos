package lsr.paxos.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.MapServiceCommand;
import lsr.service.AbstractService;

public class MapService extends AbstractService {
	private HashMap<Long, Long> _map = new HashMap<Long, Long>();
	private int _lastInstanceId = 0;

	public byte[] execute(byte[] value, int instanceId) {
		_lastInstanceId = instanceId;
		MapServiceCommand command;
		try {
			command = new MapServiceCommand(value);
		} catch (IOException e) {
			_logger.log(Level.WARNING, "Incorrect request", e);
			return null;
		}

		Long x = _map.get(command.getKey());
		if (x == null)
			x = new Long(0);

		x = command.getA() * x + command.getB();
		_map.put(command.getKey(), x);

		ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
		DataOutputStream dataOutput = new DataOutputStream(byteArrayOutput);
		try {
			dataOutput.writeLong(x);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return byteArrayOutput.toByteArray();
	}

	public int hashCode() {
		return _map.hashCode();
	}

	public void askForSnapshot(int lastSnapshotInstance) {
		forceSnapshot(lastSnapshotInstance);
	}

	public void forceSnapshot(int lastSnapshotInstance) {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(stream);
			objectOutputStream.writeObject(_map);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		fireSnapshotMade(_lastInstanceId + 1, stream.toByteArray());
	}

	@SuppressWarnings("unchecked")
	public void updateToSnapshot(int instanceId, byte[] snapshot) {
		_lastInstanceId = instanceId - 1;
		ByteArrayInputStream stream = new ByteArrayInputStream(snapshot);
		ObjectInputStream objectInputStream;
		try {
			objectInputStream = new ObjectInputStream(stream);
			_map = (HashMap<Long, Long>) objectInputStream.readObject();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	private static final Logger _logger = Logger.getLogger(MapService.class.getCanonicalName());

	public void instanceExecuted(int instanceId) {
		// ignoring
	}
}
