package lsr.analyze;

public class InstanceInfo {
    public long instanceID = 0;
    public long proposed = 0;
    public long[] replicaOrdered = {0, 0, 0};

    public String toString() {

        String result = new String();

        result = String.valueOf(instanceID);

        result = result + '\t' + proposed;

        result = result + '\t' + (replicaOrdered[0] == 0 ? "" : replicaOrdered[0]);
        result = result + '\t' + (replicaOrdered[1] == 0 ? "" : replicaOrdered[1]);
        result = result + '\t' + (replicaOrdered[2] == 0 ? "" : replicaOrdered[2]);

        return result;
    }

    public String toCsv(String tablename) {
        StringBuilder insert = new StringBuilder();

        insert.append(instanceID);

        insert.append("," + proposed);

        insert.append("," + (replicaOrdered[0] == 0 ? "" : replicaOrdered[0]));
        insert.append("," + (replicaOrdered[1] == 0 ? "" : replicaOrdered[1]));
        insert.append("," + (replicaOrdered[2] == 0 ? "" : replicaOrdered[2]));

        return insert.toString();
    }

    public String toSql(String tablename) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO " + tablename + " VALUES (");

        insert.append(instanceID);

        insert.append(", " + proposed);

        insert.append(", " + (replicaOrdered[0] == 0 ? "NULL" : replicaOrdered[0]));
        insert.append(", " + (replicaOrdered[1] == 0 ? "NULL" : replicaOrdered[1]));
        insert.append(", " + (replicaOrdered[2] == 0 ? "NULL" : replicaOrdered[2]));

        insert.append(");");
        return insert.toString();
    }

    public static String sqlCreate(String tablename) {
        StringBuilder create = new StringBuilder();
        create.append("CREATE TABLE " + tablename + "(");

        create.append("instanceID INTEGER, proposingStarted INTEGER");

        create.append(", replicaOrdered0 INTEGER, replicaOrdered1 INTEGER, replicaOrdered2 INTEGER");

        create.append(");");
        return create.toString();
    }
}
