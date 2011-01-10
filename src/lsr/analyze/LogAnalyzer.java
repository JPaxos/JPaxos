package lsr.analyze;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import lsr.common.RequestId;

public class LogAnalyzer {

    private Map<RequestId, RequestInfo> requests = new HashMap<RequestId, RequestInfo>();
    private final String tablename; // = new String("requests");
    private final Map<Long, InstanceInfo> instances = new HashMap<Long, InstanceInfo>();;

    public LogAnalyzer(String tablename) {
        this.tablename = tablename;
    }

    public void analyze(String folder, String clientPattern, String replicaPattern)
            throws IOException {

        File directory = new File(folder);
        File[] files = directory.listFiles();

        if (files == null) {
            throw new IOException("Parameter " + folder + " is not a directory!");
        }

        Pattern c = null;
        Pattern r = null;
        try {
            c = Pattern.compile(clientPattern);
            r = Pattern.compile(replicaPattern);
        } catch (PatternSyntaxException e) {
            // Hm... has someone a better idea?
            throw e;
        }

        List<File> replicaFiles = new ArrayList<File>(3);

        for (int i = 0; i < files.length; i++) {
            if (c.matcher(files[i].getName()).find()) {
                ClientAnalyzer client;
                client = new ClientAnalyzer(requests, files[i].getAbsolutePath());
                client.analyze();
            } else if (r.matcher(files[i].getName()).find()) {
                replicaFiles.add(files[i]);
            }
        }

        for (File file : replicaFiles) {
            ReplicaAnalyzer replica;
            replica = new ReplicaAnalyzer(requests, instances, file.getAbsolutePath());
            replica.analyze();
        }

        // dataToSql();
        dataToCsv();
    }

    private void dataToCsv() {
        try {
            String file1 = "tmp123456789_bu1.tmp";
            String file2 = "tmp123456789_bu2.tmp";

            System.out.println(RequestInfo.sqlCreate(tablename + "_req"));
            System.out.println(InstanceInfo.sqlCreate(tablename + "_inst"));

            FileOutputStream fos = new FileOutputStream(file1);
            PrintStream printer = new PrintStream(fos);
            for (RequestInfo info : requests.values()) {
                printer.println(info.toCsv(tablename + "_req"));
            }
            fos.close();

            fos = new FileOutputStream(file2);
            printer = new PrintStream(fos);
            for (InstanceInfo info : instances.values()) {
                printer.println(info.toCsv(tablename + "_inst"));
            }
            fos.close();

            System.out.println(".separator \",\"");
            System.out.println(".import " + file1 + " " + tablename + "_req");
            System.out.println(".import " + file2 + " " + tablename + "_inst");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unused")
    private void dataToSql() {
        System.out.println(RequestInfo.sqlCreate(tablename + "_req"));
        System.out.println(InstanceInfo.sqlCreate(tablename + "_inst"));

        for (RequestInfo info : requests.values()) {
            System.out.println(info.toSql(tablename + "_req"));
        }

        for (InstanceInfo info : instances.values()) {
            System.out.println(info.toSql(tablename + "_inst"));
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: " +
                               LogAnalyzer.class.getName() +
                               " <dir> <pattern_for_client_filename> <pattern_for_replica_filename> <sql_table_name>");
            System.exit(1);
        }

        LogAnalyzer analyzer = new LogAnalyzer(args[3]);
        try {
            analyzer.analyze(args[0], args[1], args[2]);
        } catch (Throwable t) {
            System.err.println("Error, exiting application\n");
            t.printStackTrace();
            System.exit(1);
        }
    }
}
