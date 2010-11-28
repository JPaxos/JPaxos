package lsr.paxos.messages;

import java.io.DataInputStream;
import java.io.IOException;

import lsr.leader.messages.Ping;
import lsr.leader.messages.Pong;
import lsr.leader.messages.Report;
import lsr.leader.messages.SimpleAlive;
import lsr.leader.messages.Start;

/** Determines type of the message. */
public enum MessageType {
    Accept, Alive, SimpleAlive, CatchUpQuery, CatchUpResponse, CatchUpSnapshot, Nack, Prepare,
    PrepareOK, Propose, Ping, Pong, Start, Report,
    // Special markers used by the network implementation to raise callbacks
    // There are no classes with this messages types
    ANY, // any message
    SENT; // sent messages

    public Message newInstance(DataInputStream input) throws IOException {
        assert this != ANY && this != SENT : "Message type " + this + " cannot be serialized";

        Message m;
        switch (this) {
            case Accept:
                m = new Accept(input);
                break;
            case Alive:
                m = new Alive(input);
                break;
            case SimpleAlive:
                m = new SimpleAlive(input);
                break;
            case CatchUpQuery:
                m = new CatchUpQuery(input);
                break;
            case CatchUpResponse:
                m = new CatchUpResponse(input);
                break;
            case CatchUpSnapshot:
                m = new CatchUpSnapshot(input);
                break;
            case Nack:
                m = new Nack(input);
                break;
            case Prepare:
                m = new Prepare(input);
                break;
            case PrepareOK:
                m = new PrepareOK(input);
                break;
            case Propose:
                m = new Propose(input);
                break;
            case Ping:
                m = new Ping(input);
                break;
            case Pong:
                m = new Pong(input);
                break;
            case Report:
                m = new Report(input);
                break;
            case Start:
                m = new Start(input);
                break;
            default:
                throw new IllegalArgumentException("Unknown message type given to deserialize!");
        }
        return m;
    }
}
