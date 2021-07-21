package lsr.paxos.messages;

/**
 * Represents message type.
 */
public enum MessageType {
    INVALID,

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

    ForwardedClientBatch,
    AskForClientBatch,

    ForwardedClientRequests,

    // Special markers used by the network implementation to raise callbacks
    // There are no classes with this messages types
    ANY, // any message
    SENT // sent messages
}
