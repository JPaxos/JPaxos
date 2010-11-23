package lsr.analyze;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lsr.common.RequestId;
import lsr.paxos.client.Client;

public class ClientAnalyzer {
	private long clientId = -1;

	private String fileName;
	private RequestInfo currentRequest = null;
	private final List<ConnectionInfo> connections;
	private ConnectionInfo currentConnection;

	private final Map<RequestId, RequestInfo> requests;

	public ClientAnalyzer(Map<RequestId, RequestInfo> requests, String fileName) {
		this.requests = requests;
		this.fileName = fileName;
		connections = new ArrayList<ConnectionInfo>();
	}

	public void analyze() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));

		String line;
		while (true) {
			line = reader.readLine();
			if (line == null)
				return;
			processLine(line);
		}
	}

	private void processLine(String line) {
		if (line.indexOf("New client id:") != -1)
			newClientId(line);
		if (line.indexOf("Connecting to") != -1)
			connecting(line);
		if (line.indexOf("Connection refused") != -1)
			connectionRefused(line);
		if (line.indexOf("Connected") != -1)
			connected(line);
		if (line.indexOf("Sending command") != -1)
			sendingCommand(line);
		if (line.indexOf("Reply: OK") != -1)
			replyOk(line);
	}

	private void connectionRefused(String line) {
		assert currentConnection != null : "Connection refused, but there was no try";
		Pattern pattern = Pattern.compile("(\\d+).*Could not connect to (\\d+)");
		Matcher matcher = pattern.matcher(line);
		if (!matcher.find())
			throw new LogFormatException("Connection refused");

		long time = Long.parseLong(matcher.group(1));
		int replicaId = Integer.parseInt(matcher.group(2));
		assert replicaId == currentConnection.replicaId : "Refused to connect different replica than tried";

		currentConnection.endTime = time;
		currentConnection = null;
	}

	private void connecting(String line) {
		Pattern pattern = Pattern.compile("(\\d+).*Connecting to \\[p(\\d+)\\]");
		Matcher matcher = pattern.matcher(line);
		if (!matcher.find())
			throw new LogFormatException("Connecting to");

		long time = Long.parseLong(matcher.group(1));
		int replicaId = Integer.parseInt(matcher.group(2));

		if (currentConnection != null) {
			currentConnection.endTime = time;
			connections.add(currentConnection);
		}

		currentConnection = new ConnectionInfo();
		currentConnection.startTime = time;
		currentConnection.replicaId = replicaId;

		if (currentRequest != null) {
			currentRequest.reconnectCount++;
		}
	}

	private void connected(String line) {
		assert currentConnection != null : "Connected before trying to connect";
		Pattern pattern = Pattern.compile("(\\d+).*Connected \\[p(\\d+)\\]. Timeout: (\\d+)");
		Matcher matcher = pattern.matcher(line);
		if (!matcher.find())
			throw new LogFormatException("Connected");

		long time = Long.parseLong(matcher.group(1));
		int replicaId = Integer.parseInt(matcher.group(2));
		long timeout = Integer.parseInt(matcher.group(3));

		currentConnection.connectedTime = time;
		currentConnection.replicaId = replicaId;
		currentConnection.timeout = timeout;
	}

	private void newClientId(String line) {
		assert clientId == -1 : "Got second ID";

		Pattern pattern = Pattern.compile(".*New client id: (\\d+)");
		Matcher matcher = pattern.matcher(line);
		if (matcher.find()) {
			clientId = Long.parseLong(matcher.group(1));// .trim();
		} else {
			throw new LogFormatException("Client ID pattern problem");
		}
	}

	private void sendingCommand(String line) {
		assert clientId != -1 : "ClientID is null and a command is issued!";
		Pattern pattern = Pattern.compile("(\\d+).*Sending command: REQUEST: id=(\\d+):(\\d+), value=");
		Matcher matcher = pattern.matcher(line);
		if (!matcher.find())
			throw new LogFormatException("Sending command");

		long time = Long.parseLong(matcher.group(1));
		long clientId = Long.parseLong(matcher.group(2));
		assert this.clientId == clientId : "Different clientID received form servers and different used";
		int seqeunceId = Integer.parseInt(matcher.group(3));

		RequestId rqid = new RequestId(clientId, seqeunceId);
		RequestInfo request = requests.get(rqid);

		if (request == null) {
			request = new RequestInfo();
			request.clientID = clientId;
			request.sequenceID = seqeunceId;
			request.startClient = time;
			requests.put(rqid, request);
			currentRequest = request;
		} else {
			request.instanceCount++;
		}
	}

	private void replyOk(String line) {
		assert currentRequest != null : "Got response, but no request has been issued";
		Pattern pattern = Pattern.compile("(\\d+).*Reply: OK - (\\d+):(\\d+)");
		Matcher matcher = pattern.matcher(line);
		if (!matcher.find())
			throw new LogFormatException("Reply: " + line);

		long time = Long.parseLong(matcher.group(1));
		long clientId = Long.parseLong(matcher.group(2));
		int seqeunceId = Integer.parseInt(matcher.group(3));

		assert this.clientId == clientId : "Got response for different client";
		assert seqeunceId == currentRequest.sequenceID : "Sequence Id missmatch";
		assert currentRequest == requests.get(new RequestId(clientId, seqeunceId)) : "Current request != request you got answer for";

		currentRequest.endClient = time;

		currentRequest = null;

	}

	@SuppressWarnings("unused")
	private final static Logger logger = Logger.getLogger(Client.class.getCanonicalName());
}
