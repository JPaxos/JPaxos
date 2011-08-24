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
    Nack,
    Prepare,
    PrepareOK,
    Propose,
    Recovery,
    RecoveryAnswer,

    Ping,
    Pong,
    Start,
    Report,
    
    ForwardedRequest,
    ViewPrepared,
    
    // Special markers used by the network implementation to raise callbacks
    // There are no classes with this messages types
    ANY, // any message
    SENT
    // sent messages
}
