package lsr.paxos.replica;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import lsr.common.ClientCommand;
import lsr.common.ClientCommand.CommandType;
import lsr.common.ClientReply;
import lsr.common.ClientReply.Result;
import lsr.common.PrimitivesByteArray;
import lsr.common.Reply;
import lsr.common.Request;
import lsr.common.RequestId;
import lsr.paxos.NotLeaderException;
import lsr.paxos.Paxos;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ReplicaCommandCallbackTest {
    private Paxos paxos;
    private ConcurrentHashMap<Long, Reply> lastReplies;
    private ClientProxy client;

    @Before
    public void setUp() {
        paxos = mock(Paxos.class);
        lastReplies = new ConcurrentHashMap<Long, Reply>();
        client = mock(ClientProxy.class);
    }

    @Test
    public void shouldRedirectToLeaderWhenNotALeader() throws IOException, NotLeaderException {
        when(paxos.isLeader()).thenReturn(false);
        when(paxos.getLeaderId()).thenReturn(1);

        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);
        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);
        callback.execute(command, client);

        ArgumentCaptor<ClientReply> clientReplyCaptor = ArgumentCaptor.forClass(ClientReply.class);
        verify(client).send(clientReplyCaptor.capture());
        ClientReply clientReply = clientReplyCaptor.getValue();
        assertEquals(Result.REDIRECT, clientReply.getResult());
        assertArrayEquals(PrimitivesByteArray.fromInt(1), clientReply.getValue());
        verify(paxos, never()).enqueueRequest(any(Request.class));
    }

    @Test
    public void shouldProposeNewRequest() throws IOException, NotLeaderException {
        when(paxos.isLeader()).thenReturn(true);

        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);
        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);
        callback.execute(command, client);

        verify(paxos).enqueueRequest(request);
    }

    @Test
    public void shouldRedirectToLeaderWhenLeaderIsChanging() throws NotLeaderException, IOException {
        when(paxos.isLeader()).thenReturn(true);
        when(paxos.getLeaderId()).thenReturn(1);
        doThrow(new NotLeaderException("Not a leader")).when(paxos).enqueueRequest(any(Request.class));

        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);
        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);
        callback.execute(command, client);

        ArgumentCaptor<ClientReply> clientReplyCaptor = ArgumentCaptor.forClass(ClientReply.class);
        verify(client).send(clientReplyCaptor.capture());
        ClientReply clientReply = clientReplyCaptor.getValue();
        assertEquals(Result.REDIRECT, clientReply.getResult());
        assertArrayEquals(PrimitivesByteArray.fromInt(1), clientReply.getValue());
    }

    @Test
    public void shouldDiscardAllCommandsDifferentThanRequest() throws IOException {
        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);
        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.ALIVE, request);
        callback.execute(command, client);

        ArgumentCaptor<ClientReply> clientReplyCaptor = ArgumentCaptor.forClass(ClientReply.class);
        verify(client).send(clientReplyCaptor.capture());
        ClientReply clientReply = clientReplyCaptor.getValue();
        assertEquals(Result.NACK, clientReply.getResult());
    }

    @Test
    public void shouldHandleCommandRetransmission() throws IOException {
        Reply reply = new Reply(new RequestId(0, 0), new byte[] {1, 2, 3});
        lastReplies.put((long) 0, reply);

        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);
        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);
        callback.execute(command, client);

        ArgumentCaptor<ClientReply> clientReplyCaptor = ArgumentCaptor.forClass(ClientReply.class);
        verify(client).send(clientReplyCaptor.capture());
        ClientReply clientReply = clientReplyCaptor.getValue();
        assertEquals(Result.OK, clientReply.getResult());
        assertArrayEquals(reply.toByteArray(), clientReply.getValue());
    }

    @Test
    public void shouldHandleOldCommand() throws IOException {
        Reply reply = new Reply(new RequestId(0, 1), new byte[] {1, 2, 3});
        lastReplies.put((long) 0, reply);

        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);
        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);
        callback.execute(command, client);

        ArgumentCaptor<ClientReply> clientReplyCaptor = ArgumentCaptor.forClass(ClientReply.class);
        verify(client).send(clientReplyCaptor.capture());
        ClientReply clientReply = clientReplyCaptor.getValue();
        assertEquals(Result.NACK, clientReply.getResult());
    }

    @Test
    public void shouldHandleReplyAsNotLeader() {
        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);

        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        Reply reply = new Reply(new RequestId(0, 0), new byte[] {1});

        callback.handleReply(request, reply);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void shouldHandleReplyAsLeader() throws IOException {
        when(paxos.isLeader()).thenReturn(true);
        ReplicaCommandCallback callback = new ReplicaCommandCallback(paxos, lastReplies);

        Request request = new Request(new RequestId(0, 0), new byte[] {1, 2, 3});
        Reply reply = new Reply(new RequestId(0, 0), new byte[] {1});

        ClientCommand command = new ClientCommand(CommandType.REQUEST, request);
        callback.execute(command, client);
        callback.handleReply(request, reply);

        ArgumentCaptor<ClientReply> clientReplyCaptor = ArgumentCaptor.forClass(ClientReply.class);
        verify(client).send(clientReplyCaptor.capture());
        ClientReply clientReply = clientReplyCaptor.getValue();
        assertEquals(Result.OK, clientReply.getResult());
        assertArrayEquals(reply.toByteArray(), clientReply.getValue());
    }
}
