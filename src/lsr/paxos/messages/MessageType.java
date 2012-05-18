package lsr.paxos.messages;

/**
 * Represents message type.
 */
public enum MessageType {
    Accept,
    Alive,
    SimpleAlive,
    CatchUpQuery,
    CatchUpResponse,
    CatchUpSnapshot,
	InstanceCatchUpQuery,
	InstanceCatchUpResponse,
    Nack,
    Prepare,
    PrepareOK,
    Propose,
    Recovery,
    RecoveryAnswer,
	RecoveryQuery,
	RecoveryResponse,

    Ping,
    Pong,
    Start,
    Report,
    
    ForwardedClientRequest,
    AckForwardedRequest,
    ViewPrepared,
    
    // Special markers used by the network implementation to raise callbacks
    // There are no classes with this messages types
    ANY, // any message
    SENT
    // sent messages
}
