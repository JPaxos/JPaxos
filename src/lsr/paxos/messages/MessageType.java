package lsr.paxos.messages;

/**
 * Represents message type.
 */
public enum MessageType {
    Recovery,
    RecoveryAnswer,

    Prepare,
    PrepareOK,

    Propose,
    Accept,

    Alive,

    CatchUpQuery,
    CatchUpResponse,
    CatchUpSnapshot,

    ForwardedClientRequest,
    AckForwardedRequest,

    // Special markers used by the network implementation to raise callbacks
    // There are no classes with this messages types
    ANY, // any message
    SENT // sent messages
}
