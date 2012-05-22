package lsr.paxos;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Random;

import lsr.common.ClientBatch;
import lsr.common.Configuration;
import lsr.common.Pair;
import lsr.common.PriorityTask;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.messages.CatchUpQuery;
import lsr.paxos.messages.CatchUpResponse;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.RecoveryQuery;
import lsr.paxos.messages.RecoveryResponse;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Storage;
import lsr.paxos.replica.Replica;
import lsr.paxos.replica.Snapshot;

public class CatchUp {

    private Storage storage;
    private Network network;
    private Paxos paxos;
	private Replica replica;
	
	private HashMap<Integer, Integer> catchUpResponses;

    private SingleThreadDispatcher dispatcher;
	
	private int catchUpId = 0;
	private ScheduledFuture catchUpTask;
	private ScheduledFuture recoverTask;


    /** Holds all listeners that want to know about catch-up state change */
    HashSet<CatchUpListener> listeners = new HashSet<CatchUpListener>();

    public CatchUp(Paxos paxos, Storage storage, Network network) {
        this.network = network;
        this.dispatcher = paxos.getDispatcher();
        MessageHandler handler = new InnerMessageHandler();
        Network.addMessageListener(MessageType.CatchUpQuery, handler);
        Network.addMessageListener(MessageType.CatchUpResponse, handler);
        Network.addMessageListener(MessageType.RecoveryQuery, handler);
        Network.addMessageListener(MessageType.RecoveryResponse, handler);

        this.paxos = paxos;
        this.storage = storage;
    }

    public void start() {
        // TODO: Automatic catch-up is disabled for the time being. Catch-up is done on demand only.
    }

    /** Called to initiate catchup. */
    public void startCatchup() {
        doCatchUp();
    }

    public void forceCatchup() {
        doCatchUp();
    }

	void doCatchUp() {
        assert dispatcher.amIInDispatcher() : "Must be running on the Protocol thread";

        if (paxos.isLeader()) {
            logger.warning("Ignoring catchup request. Replica is in leader role");
            return;
        }
                
        logger.info("Starting catchup");
		
		catchUpResponses = new HashMap<Integer, Integer>();
		catchUpId++;

        CatchUpQuery query = new CatchUpQuery(storage.getView(), new int[0], catchUpId);
		fillUnknownList(query);
        network.sendToAll(query);
		logger.info("Sent " + query.toString() + " to [all]");
		
		if(catchUpTask != null){ // cancel previous ones
			if(catchUpTask.cancel(true))
				logger.info("CatchUpQuery: cancel previous task successfully");
			else
				logger.info("CatchUpQuery: cancel previous task failed");
		}
		catchUpTask = dispatcher.schedule(new Runnable() { // needed if the query is not received 
			@Override
			public void run() {
				logger.info("LISA: doCatchUp LILI");
				doCatchUp();
			}
		}, 500, TimeUnit.MILLISECONDS);
    }
	
	/**
     * Generates (ascending) list of instance numbers, which we consider for
     * undecided yet on basis of the log, and adds the next instance number on
     * the end of this list
     * 
     * @param query
     * @return count of instances embedded
     */
	
    private void fillUnknownList(CatchUpQuery query) {
        query.setInstanceIdList(findUnknownList());
	}
	
	private void fillUnknownList(RecoveryQuery query) {
        query.setInstanceIdList(findUnknownList());
	}
/*
	private List<Integer> findUnknownList(){
		List<Integer> unknownList = new ArrayList<Integer>();
        SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();
		String s = "";
		
        if (!log.isEmpty()) {
        
			int begin = -1;
			boolean previous = false;
			int lastKey = log.lastKey();
		
			ConsensusInstance instance;
			for (int i = Math.max(storage.getFirstUncommitted(), log.firstKey()); i <= lastKey; ++i) {
				instance = log.get(i);
			
				if (instance == null) {
					continue;
				}
			
				if (instance.getState() != LogEntryState.DECIDED) {
					if (!previous) {
						begin = i;
						previous = true;
					}
				} else if (previous) {
					assert begin != -1 : "Problem in unknown list creation 1";
					if (begin == i - 1) {
						unknownList.add(begin);
						s = s+" "+begin;
					}
					previous = false;
				}
			}
		
			if (previous) {
				assert begin != -1 : "Problem in unknown list creation 2";
				if (begin == lastKey) {
					unknownList.add(begin);
					s = s+" "+begin;
				}
			}
		
			int last = lastKey+1;
			unknownList.add(last);
			s = s+" "+last;
			logger.info("LISA " +catchUpId+": CatchUpQuery. Missing: "+s);
			return unknownList;
		}
		return null;
	}
	*/
	
	private List<Integer> findUnknownList(){
		List<Integer> unknownList = new ArrayList<Integer>();
        SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();
		String s = "";
		
        if (!log.isEmpty()) {
			int lastKey = log.lastKey();
			
			ConsensusInstance instance;
			for (int i = Math.max(storage.getFirstUncommitted(), log.firstKey()); i <= lastKey; ++i) {
				instance = log.get(i);
				if (instance == null || instance.getState() != LogEntryState.DECIDED) {
					unknownList.add(i);
					s = s+" "+i;
				}
			}
			logger.info("LISA " +catchUpId+": CatchUpQuery. Missing: "+s);
			return unknownList;
		}
		return null;
	}
	
    private class InnerMessageHandler implements MessageHandler {
        public void onMessageReceived(final Message msg, final int sender) {
            dispatcher.submit(new Runnable() {
                public void run() {
                    switch (msg.getType()) {
							
						case CatchUpQuery:
							handleCatchUpQuery((CatchUpQuery) msg, sender);
                            break;	
							
                        case CatchUpResponse:
							handleCatchUpResponse((CatchUpResponse) msg, sender);
                            break;                       
							
						case RecoveryQuery:
							handleRecoveryQuery((RecoveryQuery) msg, sender);
                            break;
							
                        case RecoveryResponse:
							handleRecoveryResponse((RecoveryResponse) msg, sender);
                            break;
						
                        default:
                            assert false : "Unexpected message type: " + msg.getType();
                    }
                }
            });
        }
		
		public void onMessageSent(Message message, BitSet destinations) {
        }
		
		public void handleCatchUpQuery(CatchUpQuery query, int sender){
			if (paxos.isLeader()) {
				logger.info("CatchUpQuery received: "+paxos.getLocalId()+" is Leader and can not answer catch-up request");
				return;
			}
			
			if(sender == paxos.getLocalId())
				return;
						
			int result = canHelpCatchUp(query); // intersection of missing instances in the two logs
			logger.info("LISA " +catchUpId+": CatchUpQuery received: "+paxos.getLocalId()+" has "+result+" requested instances missing.");
			CatchUpResponse response = new CatchUpResponse(storage.getView(),result, query.getCatchUpId()); // sends the number of missing instances
			network.sendMessage(response, sender);
		}
		
		public void handleCatchUpResponse(CatchUpResponse response, int sender){
			
			if(catchUpTask != null){
				if(catchUpTask.cancel(true))
					logger.info("CatchUpResponse: cancel previous task successfully");
				else
					logger.info("CatchUpResponse: cancel previous task failed");
			}
			
			if(response.getCatchUpId() == catchUpId) {  // Test if the response is for this catch-up or one of the previous ones
				catchUpResponses.put(sender, response.getMissingInstances());
				logger.info("LISA " +catchUpId+": CatchUpResponse received. "+sender+" has "+response.getMissingInstances() + " missing instances");
				
				int n = storage.getView()+1;
				int f = (n-1)/2;
				if(catchUpResponses.size() >= 1) {// waiting for n-f-1 responses (n-f)-leader // (n-f-1)
					int best = selectBestReplicaToCatchUp();
					logger.info("LISA " +catchUpId+": CatchUpResponse: "+best+" has been selected to catch-up from");
					RecoveryQuery query = new RecoveryQuery(storage.getView(), new int[0], response.getCatchUpId());		
					fillUnknownList(query);
					network.sendMessage(query, best);
					
					if(recoverTask != null){ // cancel the previous ones
						if(recoverTask.cancel(true))
							logger.info("RecoveryQuery: cancel previous task successfully");
						else
							logger.info("RecoveryQuery: cancel previous task failed");
					}
					recoverTask = dispatcher.schedule(new Runnable() {
						@Override
						public void run() {
							logger.info("LISA: doCatchUp LALA");
							doCatchUp();
						}
					}, 500, TimeUnit.MILLISECONDS);
				}
			}
			
        }
		
		public void handleRecoveryQuery(RecoveryQuery query, int sender){
			int[] instanceIdArray = query.getInstanceIdArray();
			ConsensusInstance instance;
			logger.info("LISA " +catchUpId+": RecoveryQuery: first needed is: "+instanceIdArray[0]);
			logger.info("LISA " +catchUpId+": RecoveryQuery: first has is: "+storage.getFirstUncommitted());

			if(instanceIdArray != null) {
				if (instanceIdArray[0] < storage.getFirstUncommitted()) { // send snapshot
					Snapshot snapshot = replica.getSnapshot();
					logger.info("LISA " +catchUpId+": RecoveryQuery: sends snapshot for instance: "+snapshot.getHandle().getPaxosInstanceId());
					RecoveryResponse response = new RecoveryResponse(storage.getView(), snapshot.getHandle().getPaxosInstanceId() ,snapshot.getData(), true, query.getCatchUpId());								
					network.sendMessage(response, sender);
				}
				logger.info("LISA " +catchUpId+": RecoveryQuery: start sending log entries");
				for (int instanceId : instanceIdArray) { // send log entries one by one
					logger.info("LISA instanceId >= storage.getFirstUncommitted(): " +instanceId+" "+storage.getFirstUncommitted());
					if (instanceId < storage.getFirstUncommitted() && instanceId >= storage.getLog().getLowestAvailableId()) {
						logger.info("LISA " +catchUpId+": RecoveryQuery: sending instance: "+instanceId);
						instance = storage.getLog().getInstanceMap().get(instanceId);
						if (instance != null && instance.getState() == LogEntryState.DECIDED) {
							RecoveryResponse response = new RecoveryResponse(storage.getView(), 0, instance.toByteArray(), false, query.getCatchUpId());								
							network.sendMessage(response, sender);
						}
					}
				}
				logger.info("RecoveryQuery: finished sending log entries");
			}
		}
		
		public void handleRecoveryResponse(RecoveryResponse response, int sender){
			if(recoverTask != null){
				if(recoverTask.cancel(true))
					logger.info("RecoveryResponse: cancel previous task successfully");
				else
					logger.info("RecoveryResponse: cancel previous task failed");
			}
			
			if(response.getCatchUpId() == catchUpId) {
				final int paxosId = response.getPaxosId();
				final byte[] data = response.getData();
				
				if (response.isSnapshot()){
					
					logger.info("LISA " +catchUpId+": RecoveryResponse: got a snapshot for paxosId: "+paxosId);
					replica.getDispatcher().submit(new Runnable() {
						@Override
						public void run() {
							replica.installSnapshot(paxosId, data);
						}
					}  );
					//storage.getLog().truncateBelow(paxosId); // truncate second log?
					//storage.updateFirstUncommitted();
					logger.info("LISA " +catchUpId+": Install snapshot SUCCESS");
					
				} else { // check if received snapshot before and if state recovery finished?
					
					try {
						ByteArrayInputStream bis = new ByteArrayInputStream(data);
						DataInputStream dis = new DataInputStream(bis);
						ConsensusInstance inst = new ConsensusInstance(dis);
						logger.info("LISA " +catchUpId+": RecoveryResponse: got a log entry number: "+inst.getId());

						ConsensusInstance localLog = storage.getLog().getInstance(paxosId);
						if (localLog == null){
							localLog = inst;
							if(localLog.getState() != LogEntryState.DECIDED) {
								localLog.setDecided();
							}
							Deque<ClientBatch> requests = Batcher.unpack(localLog.getValue());
							paxos.getDecideCallback().onRequestOrdered(paxosId, requests);
							//storage.updateFirstUncommitted(); ?
							logger.info("LISA " +catchUpId+": Install log SUCCESS");
						}
					}
					catch (IOException e) {
						logger.warning("RecoveryResponse: could not retreive the ConsensusInstance");
					}
				}
			}
		}
		
		public int canHelpCatchUp(CatchUpQuery query) {			
			
			SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();
			ConsensusInstance instance;
			int count = 0;
			
			if(query.getInstanceIdArray() == null) 
				return 0;
			
			for (int instanceId : query.getInstanceIdArray()) {
				if (instanceId >= storage.getFirstUncommitted()) {
					instance = log.get(instanceId);
					if (instance == null || instance.getState() != LogEntryState.DECIDED) {
						count++;
					}
				}
			}
			
			return count;
		}
    }
	
	/*
	 * Optimise best selection here
	 */
	public int selectBestReplicaToCatchUp(){
		int best = 0;
		int missedInstances;
		int leastMissedInstances = -1;
		
		for(int sender : catchUpResponses.keySet()){
			missedInstances = catchUpResponses.get(sender);
			if(leastMissedInstances == -1){
				leastMissedInstances = missedInstances;
				best = sender;
			}
			if(missedInstances < leastMissedInstances){
				best = sender;
				leastMissedInstances = missedInstances;
			}
		}
		return best;
	}
	
	private void checkCatchupSucceded() {
        if (assumeSucceded()) {
            logger.info("Catch-up succeeded");
            for (CatchUpListener listener : listeners) {
                listener.catchUpSucceeded();
            }            
        }
    }
	
    private boolean assumeSucceded() {
        // the current is OK for catch-up caused by the window violation, and
        // not for periodical one. Consider writing a method giving more sense
        return storage.isInWindow(storage.getLog().getNextId() - 1);
    }
	
    /** Adds the listener, returns if succeeded */
    public boolean addListener(CatchUpListener listener) {
        return listeners.add(listener);
    }

    /** Removes the listener, returns if succeeded */
    public boolean removeListener(CatchUpListener listener) {
        return listeners.remove(listener);
    }
	
	public void setReplica (Replica replica) {
        this.replica = replica;
    }

    private final static Logger logger = Logger.getLogger(CatchUp.class.getCanonicalName());
}
