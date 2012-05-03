package lsr.paxos;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lsr.common.Configuration;
import lsr.common.Pair;
import lsr.common.ProcessDescriptor;
import lsr.common.SingleThreadDispatcher;
import lsr.paxos.messages.CatchUpQuery;
import lsr.paxos.messages.CatchUpResponse;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.Storage;

public class CatchUp {

    private Storage storage;
    private Network network;
    private Paxos paxos;

	private HashMap catchUpResponses;

    private SingleThreadDispatcher dispatcher;

    /**
     * Current CatchUp run mode - either requesting snapshot, or requesting
     * instances
     */
    private Mode mode = Mode.Normal;

    private enum Mode {
        Normal, Snapshot
    };

    /** Holds all listeners that want to know about catch-up state change */
  //  HashSet<CatchUpListener> listeners = new HashSet<CatchUpListener>();

    public CatchUp(Paxos paxos, Storage storage, Network network) {
        this.network = network;
        this.dispatcher = paxos.getDispatcher();
        MessageHandler handler = new InnerMessageHandler();
        Network.addMessageListener(MessageType.CatchUpQuery, handler);
        Network.addMessageListener(MessageType.CatchUpResponse, handler);
        Network.addMessageListener(MessageType.CatchUpSnapshot, handler);

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
		
		catchUpResponses = new HashMap();
		
		int numReplicas = ProcessDescriptor.getInstance().numReplicas;
		BitSet targets = new BitSet(numReplicas);
		targets.set(0, numReplicas);

        CatchUpQuery query = new CatchUpQuery(storage.getView(), new int[0]);
		fillUnknownList(query);
        network.sendMessage(query, targets);
		logger.info("Sent " + query.toString() + " to [all: p" + targets + "]");
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
        List<Integer> unknownList = new ArrayList<Integer>();
        SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();
		
        if (log.isEmpty()) {
            return;
        }
		
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
        query.setInstanceIdList(unknownList);
	}

    private class InnerMessageHandler implements MessageHandler {
        public void onMessageReceived(final Message msg, final int sender) {
            dispatcher.submit(new Runnable() {
                public void run() {
                    switch (msg.getType()) {
                        case CatchUpResponse:
							 // int best;
							 // catchUpResponses.put(senderId, query.getSnapshot());
							 // if(catchUpResponses.size() == n || (timeout passé && catchUpResponses.size() > 0)) { // ou f+1
							 //		best = selectBestReplicaToCatchUp();
							 //		send recovery Query
							// }
							 // TIMEOUT doCatchUp(); and empty catchUpResponses // Ou rien du tout?
							
							// Tester si vieux, reçoit déjà? Id?
							
                            break;
							
                        case CatchUpQuery:
							if (paxos.isLeader()) {
								logger.info("CatchUpQuery received: "+paxos.getLocalId()+" is Leader and can't answer catch-up request");
								return;
							}
							
							CatchUpQuery query = (CatchUpQuery) msg;
							paxos.setTruncatePermitted(false);
							
							int result = canHelpCatchUp(query);
							logger.info("CatchUpQuery received: "+paxos.getLocalId()+" has "+result+" requested instances missing.");
							CatchUpResponse response = new CatchUpResponse(storage.getView(),result);								
							network.sendMessage(response, sender);
							
							// LISA TIMEOUT truncatePermitted = true; ?
							
                            break;
							
						//case RecoveryQuery:
                            // while(tant qu'il faut envoyer des messages)
							//		network.send(new RecoveryResponse(snapshot or part of log));
							// renvoyer un truncate permitted à true à tous les autres?
							
                        //    break;
							
                        //case RecoveryResponse:
                            // if (query.getObject().type() == snapshot){
							//	dispatch to replica
							// } else if (query.getObject().type() == log) {
							//	if(firstUncomitted() == log.getId()) { // Tester si vieux, reçoit déjà? Id?
							//		execute the log
							//		put in log
							//	}
							// }
							// Si j'ai tout, renvoyer truncate permitted à true ou timeout tout seul?
							
                        //    break;
							
                        default:
                            assert false : "Unexpected message type: " + msg.getType();
                    }
                }
            });
        }
		
		public void onMessageSent(Message message, BitSet destinations) {
        }
		
		public int canHelpCatchUp(CatchUpQuery query) {			
			
			SortedMap<Integer, ConsensusInstance> log = storage.getLog().getInstanceMap();
			ConsensusInstance instance;
			int count = 0;
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
	
	
	/* LISA See class Paxos > Recovery >  RecoveryCatchUp */

    /** Adds the listener, returns if succeeded */
/*    public boolean addListener(CatchUpListener listener) {
        return listeners.add(listener);
    }*/

    /** Removes the listener, returns if succeeded */
/*    public boolean removeListener(CatchUpListener listener) {
        return listeners.remove(listener);
    }*/

    private final static Logger logger = Logger.getLogger(CatchUp.class.getCanonicalName());
}
