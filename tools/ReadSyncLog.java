import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

class ReadSyncLog{

/* * Record types * */
    /* Sync */
    private static final byte CHANGE_VIEW = 0x01;
    private static final byte CHANGE_VALUE = 0x02;
    private static final byte SNAPSHOT = 0x03;
    /* Async */
    private static final byte DECIDED = 0x21;

public static void main (String[]args) throws Exception{
  File file = new File(args[0]);
    

DataInputStream stream = new DataInputStream(new FileInputStream(file));

        while (true) {
                int type = stream.read();
                if (type == -1) {
                    break;
                }
                int id = stream.readInt();

                switch (type) {
                    case CHANGE_VIEW: {
                        int view = stream.readInt();
                        System.out.println(String.format("%8s %5d %3d", "view", id, view));
                        break;
                    }
                    case CHANGE_VALUE: {
                        int view = stream.readInt();
                        int length = stream.readInt();
                        byte[] value;
                        if (length == -1) {
                            value = null;
                        } else {
                            value = new byte[length];
                            stream.readFully(value);
                        }
                        System.out.println(String.format("%8s %5d %3d", "value", id, length));
                        break;
                    }
                    case DECIDED: {
                        System.out.println(String.format("%8s %5d", "decided", id));
                        break;
                    }
                    case SNAPSHOT: {
                        System.out.println("== snashot == (" + id + ")");
                        break;
                    }
                    default:
                        assert false : "Unrecognized log record type";
                }
        }
        stream.close();
}}