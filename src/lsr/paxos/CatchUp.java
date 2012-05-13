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

import lsr.common.ClientBatch;
import lsr.common.Configuration;
import lsr.common.Pair;
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

public class CatchUp {

    private Storage storage;
    private Network network;
    private Paxos paxos;
	private Replica replica;
	
	private HashMap<Integer, Integer> catchUpResponses;

    private SingleThreadDispatcher dispatcher;
	
	private int catchUpId = 0;


    /** Holds all listeners that want to know about catch-up state change */
  //  HashSet<CatchUpListener> listeners = new HashSet<CatchUpListener>();

    public CatchUp(Paxos paxos, Storage storage, Network network) {
        this.network = network;
        this.dispatcher = paxos.getDispatcher();
        MessageHandler handler = new InnerMessageHandler();
        Network.addMessageListener(MessageType.CatchUpQuery, handler);
        Network.addMessageListener(MessageType.CatchUpResponse, handler);
        Network.addMessageListener(MessageType.RecoveryQuery, handler);

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
		
		catchUpId++;
		catchUpResponses = new HashMap<Integer, Integer>();
		
		/*int numReplicas = ProcessDescriptor.getInstance().numReplicas;
		BitSet targets = new BitSet(numReplicas);
		targets.set(0, numReplicas);
		network.sendMessage(query, targets);*/

        CatchUpQuery query = new CatchUpQuery(storage.getView(), new int[0], catchUpId);
		fillUnknownList(query);
        network.sendToAll(query);
		logger.info("Sent " + query.toString() + " to [all]");
		/* LISA see catch-up query Modify CatchUpQuery? */
    }
	
	
	/**
     * Generates (ascending) list of instance numbers, which we consider for
     * undecided yet on basis of the log, and adds the next instance number on
     * the end of this list
     * 
     * @param query
     * @return count of instances embedded
     */
	
	/*
	 TODO: 2nd recovery: if the second log is not yet filled
	 */
    private void fillUnknownList(CatchUpQuery query) {
        query.setInstanceIdList(findUnknownList());
	}
	
	private void fillUnknownList(RecoveryQuery query) {
        query.setInstanceIdList(findUnknownList());
	}

	private List<Integer> findUnknownList(){
		List<Integer> unknownList = new ArrayList<Integer>();
        SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();
		
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
					}
					previous = false;
				}
			}
		
			if (previous) {
				assert begin != -1 : "Problem in unknown list creation 2";
				if (begin == lastKey) {
					unknownList.add(begin);
				}
			}
		
			unknownList.add(lastKey + 1);
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
						
			int result = canHelpCatchUp(query); // intersection of missing instances in the two logs
			logger.info("CatchUpQuery received: "+paxos.getLocalId()+" has "+result+" requested instances missing.");
			CatchUpResponse response = new CatchUpResponse(storage.getView(),result, query.getCatchUpId()); // sends the number of missing instances
			network.sendMessage(response, sender);
						
		}
		
		public void handleCatchUpResponse(CatchUpResponse response, int sender){
			if(response.getCatchUpId() == catchUpId) {  // Test if the response is for this catch-up or one of the previous ones
				catchUpResponses.put(sender, response.getMissingInstances());
				logger.info("CatchUpResponse received. "+sender+" has "+response.getMissingInstances() + "missing instances");
				if(catchUpResponses.size() == (storage.getView()-(storage.getView()-1)/2)) {// waiting for n-f responses
					int best = selectBestReplicaToCatchUp();
					logger.info("CatchUpResponse: "+best+" has been selected to catch-up from");
					RecoveryQuery query = new RecoveryQuery(storage.getView(), new int[0], response.getCatchUpId());		
					fillUnknownList(query);
					network.sendMessage(query, best);
				}
			}
			
        }
		
		public void handleRecoveryQuery(RecoveryQuery query, int sender){
			int[] instanceIdArray = query.getInstanceIdArray();
			ConsensusInstance instance;
			
			if(instanceIdArray != null) {
				if (instanceIdArray[0] < storage.getFirstUncommitted()) { // send snapshot
					logger.info("RecoveryQuery: sends snapshot");
					RecoveryResponse res = new RecoveryResponse(storage.getView(), replica.getSnapshot().getHandle().getPaxosInstanceId() ,replica.getSnapshot().getData(), true, query.getCatchUpId());								
					network.sendMessage(res, sender);
				}
				logger.info("RecoveryQuery: start sending log entries");
				for (int instanceId : instanceIdArray) { // send log entries one by one
					if (instanceId >= storage.getFirstUncommitted()) {
						instance = storage.getLog().getInstanceMap().get(instanceId);
						if (instance != null && instance.getState() == LogEntryState.DECIDED) {
							RecoveryResponse res = new RecoveryResponse(storage.getView(), 0, instance.toByteArray(), false, query.getCatchUpId());								
							network.sendMessage(res, sender);
						}
					}
				}
				logger.info("RecoveryQuery: finished sending log entries");
			}
		
		}
		
		public void handleRecoveryResponse(RecoveryResponse response, int sender){
			if(response.getCatchUpId() == catchUpId) {
				final int paxosId = response.getPaxosId();
				final byte[] data = response.getData();
				
				if (response.isSnapshot()){
					
					logger.info("RecoveryResponse: got a snapshot");
					replica.getDispatcher().submit(new Runnable() {
						@Override
						public void run() {
							replica.installSnapshot(paxosId, data);
						}
					}  );
					storage.getLog().truncateBelow(paxosId); // truncate second log?
					storage.updateFirstUncommitted();
					
				} else { // check if received snapshot before and if state recovery finished?
					
					logger.info("RecoveryResponse: got a log entry");
					try {
						ByteArrayInputStream bis = new ByteArrayInputStream(data);
						DataInputStream dis = new DataInputStream(bis);
						ConsensusInstance inst = new ConsensusInstance(dis);
						if(storage.getFirstUncommitted() == inst.getId()) { // really needed?
							ConsensusInstance localLog = storage.getLog().getInstance(paxosId);
							localLog.setDecided();
							Deque<ClientBatch> requests = Batcher.unpack(localLog.getValue()); // why value?
							paxos.getDecideCallback().onRequestOrdered(paxosId, requests);
							// storage.updateFirstUncommitted(); ?
						}
					}
					catch (IOException e) {
						logger.warning("RecoveryResponse: could not retreive the ConsensusInstance");
					}
					
				}
				// Si j'ai tout, renvoyer truncate permitted à true ou timeout tout seul?
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
		int missedInstances = catchUpResponses.get(0);
		int leastMissedInstances = missedInstances;
		
		for(int sender : catchUpResponses.keySet()){
			missedInstances = catchUpResponses.get(sender);
			if(missedInstances < leastMissedInstances){
				best = sender;
				leastMissedInstances = missedInstances;
			}
		}
		return best;
	}
	
	
	/* LISA See class Paxos > Recovery >  RecoveryCatchUp */

    /** Adds the listener, returns if succeeded */
/*    public boolean addListener(CatchUpListener listener) {
        return listeners.add(listener);
    }*/

    /** Removes the listener, returns if succeeded */
/*    public boolean removeListener(CatchUpListener listener) {
        return listeners.remove(listener);
    }*/
	
	public void setReplica (Replica replica) {
        this.replica = replica;
    }

    private final static Logger logger = Logger.getLogger(CatchUp.class.getCanonicalName());
}
