package lsr.analyze;

public class RequestInfo {
    public long clientID;
    public int sequenceID;
    public long startClient;
    public long endClient;
    public long reconnectCount = 0;

    // public long proposingStarted;

    public long instanceID = -1;

    public int instanceCount = 1;

    public long[] replicaExecuted = {0, 0, 0};

    public String toString() {
        // I hope the JVM will do it's job well

        String result = new String();

        result = result + clientID + '\t' + sequenceID;

        result = result + '\t' + startClient + '\t' + endClient;

        result = result + '\t' + reconnectCount;

        result = result + '\t' + instanceID;

        result = result + '\t' + instanceCount;

        result = result + '\t' + replicaExecuted[0];
        result = result + '\t' + replicaExecuted[1];
        result = result + '\t' + replicaExecuted[2];

        return result;
    }

    public String toCsv(String tablename) {
        StringBuilder insert = new StringBuilder();

        insert.append(clientID + "," + sequenceID + ",");
        insert.append(startClient + "," + endClient + ",");

        insert.append(reconnectCount + "," + instanceID + ",");
        insert.append(instanceCount);

        insert.append("," + (replicaExecuted[0] == 0 ? "" : replicaExecuted[0]));
        insert.append("," + (replicaExecuted[1] == 0 ? "" : replicaExecuted[1]));
        insert.append("," + (replicaExecuted[2] == 0 ? "" : replicaExecuted[2]));

        return insert.toString();
    }

    public String toSql(String tablename) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO " + tablename + " VALUES (");

        insert.append(clientID + ", " + sequenceID + ", ");
        insert.append(startClient + ", " + endClient + ", ");

        insert.append(reconnectCount + ", " + instanceID + ", ");
        insert.append(instanceCount);

        insert.append("," + (replicaExecuted[0] == 0 ? "NULL" : replicaExecuted[0]));
        insert.append("," + (replicaExecuted[1] == 0 ? "NULL" : replicaExecuted[1]));
        insert.append("," + (replicaExecuted[2] == 0 ? "NULL" : replicaExecuted[2]));

        insert.append(");");
        return insert.toString();
    }

    public static String sqlCreate(String tablename) {
        StringBuilder create = new StringBuilder();
        create.append("CREATE TABLE " + tablename + "(");

        create.append("clientID INTEGER, sequenceID INTEGER, startClient INTEGER, endClient INTEGER, ");

        create.append("reconnectCount INTEGER, instanceID INTEGER, instanceCount INTEGER");

        create.append(", replicaExecuted0 INTEGER, replicaExecuted1 INTEGER, replicaExecuted2 INTEGER");

        create.append(");");
        return create.toString();
    }
}
