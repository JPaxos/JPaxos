package lsr.paxos.network;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

import lsr.common.PID;

public abstract class NioConnection extends Thread
{
	protected final static int TCP_BUFFER_SIZE = 4 * 1024 * 1024;

	protected NioNetwork network;
	protected SocketChannel channel;
	protected Selector selector;
	
	protected final PID replica;
	
	protected volatile boolean stop = false;

	public NioConnection(NioNetwork network, PID replica, SocketChannel channel)
			throws IOException
	{
		this.network = network;
		this.replica = replica;
		this.channel = channel;
		selector = SelectorProvider.provider().openSelector();
		this.setDaemon(true);
	}

	@Override
	public abstract void run();
	
	protected abstract void disposeConnection();
	
	protected boolean isActiveConnection()
	{
		return Network.localId < replica.getId();
	}
}
