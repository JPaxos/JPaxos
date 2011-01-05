package lsr.paxos.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Responsible for reading and writing single number to file.
 */
public class SingleNumberWriter {
    private final String filePath;

    public SingleNumberWriter(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Reads number from file. If the file doesn't exist, returns 0.
     * 
     * @return number from file or 0 if file doesn't exist
     */
    public long readNumber() {
        File file = new File(filePath);
        if (!file.exists())
            return 0;

        long number;
        try {
            DataInputStream stream = new DataInputStream(new FileInputStream(file));
            number = stream.readLong();
            stream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return number;
    }

    public void writeNumber(long number) {
        new File(filePath).getParentFile().mkdirs();
        File tempFile = new File(filePath + "_t");

        try {
            tempFile.createNewFile();
            DataOutputStream stream = new DataOutputStream(new FileOutputStream(tempFile, false));
            stream.writeLong(number);
            stream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (!tempFile.renameTo(new File(filePath)))
            throw new RuntimeException("Could not replace the file with saved number!");
    }
}
