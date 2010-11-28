package lsr.analyze;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lsr.common.RequestId;

public class ReplicaAnalyzer {

    private final Map<RequestId, RequestInfo> requests;
    private final String filename;

    private int replicaId = -1;
    private final Map<Long, InstanceInfo> instances;

    public ReplicaAnalyzer(Map<RequestId, RequestInfo> requests, Map<Long, InstanceInfo> instances,
                           String filename) {
        this.requests = requests;
        this.instances = instances;
        this.filename = filename;

        Matcher replicaIdMatcher = Pattern.compile("(\\d+)$").matcher(filename);

        if (!replicaIdMatcher.find()) {
            throw new RuntimeException("No info about replica no in the file");
        }

        replicaId = Integer.parseInt(replicaIdMatcher.group());

        replicaId--;

        if (replicaId < 0 || replicaId > 2) {
            throw new RuntimeException("Please modify the code to support more then three replicas");
        }
    }

    public void analyze() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));

        String line;
        while (true) {
            line = reader.readLine();
            if (line == null)
                return;
            processLine(line);
        }
    }

    private void processLine(String line) {
        if (line.indexOf("Proposing:") != -1)
            proposing(line);
        if (line.indexOf("Executed") != -1)
            executed(line);
        if (line.indexOf("ordered") != -1)
            ordered(line);
    }

    private void executed(String line) {
        // Executed #12, id=1249996780573023:5, req=[B@982589, reply=[B@c88440
        Pattern pattern = Pattern.compile("(\\d+) .*#(\\d+), id=(\\d+):(\\d+),");
        Matcher matcher = pattern.matcher(line);
        if (!matcher.find())
            throw new LogFormatException("Executed " + line);

        RequestInfo rInfo = requests.get(new RequestId(Long.parseLong(matcher.group(3)),
                Integer.parseInt(matcher.group(4))));

        assert rInfo != null : "Handling request sent outside the test!";

        long instanceID = Long.parseLong(matcher.group(2));
        InstanceInfo iInfo = instances.get(instanceID);

        assert (iInfo != null) && (iInfo.replicaOrdered[replicaId] != 0) : "Executing unordered instance! " +
                                                                           filename + ": " + line;

        assert rInfo.replicaExecuted[replicaId] == 0 : "Executing twice!" + line;

        rInfo.replicaExecuted[replicaId] = Long.parseLong(matcher.group(1));

        rInfo.instanceID = instanceID;

    }

    private void ordered(String line) {
        Pattern pattern = Pattern.compile("(\\d+) .*: (\\d+):");
        Matcher matcher = pattern.matcher(line);
        if (!matcher.find())
            throw new LogFormatException("Ordered " + line);

        long instance = Long.parseLong(matcher.group(2));

        InstanceInfo info = instances.get(instance);
        if (info == null) {
            info = new InstanceInfo();
            info.instanceID = instance;
            instances.put(info.instanceID, info);
        }

        assert info.replicaOrdered[replicaId] == 0 : "Ordered same instance twice!";

        info.replicaOrdered[replicaId] = Long.parseLong(matcher.group(1));

    }

    private void proposing(String line) {
        Pattern pattern = Pattern.compile("(\\d+) .*Instance=(\\d+),");
        Matcher matcher = pattern.matcher(line);
        if (!matcher.find())
            throw new LogFormatException("Proposing " + line);

        long instanceID = Long.parseLong(matcher.group(2));

        InstanceInfo info;
        info = instances.get(instanceID);

        if (info == null) {

            info = new InstanceInfo();
            info.instanceID = Long.parseLong(matcher.group(2));
            info.proposed = Long.parseLong(matcher.group(1));

            instances.put(info.instanceID, info);
        } else {
            if (info.proposed == 0)
                info.proposed = Long.parseLong(matcher.group(1));
            else
                info.proposed = Math.min(Long.parseLong(matcher.group(1)), info.proposed);
        }
    }
}
