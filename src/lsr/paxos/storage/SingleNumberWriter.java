package lsr.paxos.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Responsible for reading and writing single number to file.
 */
public class SingleNumberWriter {
    private final String directoryPath;
    private final String filePrefix;
    private int fileNumber;

    /**
     * Creates new instance of <code>SingleNumberWriter</code>.
     * <p>
     * Creates files of following format:
     * 
     * <pre>
     * /directoryPath/filePrefix.0
     * /directoryPath/filePrefix.1
     * /directoryPath/filePrefix.2
     * ...
     * </pre>
     * 
     * <p>
     * Note: directoryPath doesn't have to exist. If it doesn't, it will be
     * created automatically.
     * 
     * @param directoryPath - the directory where files will be created.
     * @param filePrefix - the prefix of file name
     */
    public SingleNumberWriter(String directoryPath, String filePrefix) {
        this.directoryPath = directoryPath;
        this.filePrefix = filePrefix;

        // prepare
        new File(directoryPath).mkdirs();
        fileNumber = getLastFileNumber(new File(directoryPath).list());
    }

    /**
     * Reads number from file. If the file doesn't exist, returns 0.
     * 
     * @return number from file or 0 if file doesn't exist
     */
    public long readNumber() {
        File file = new File(currentFilePath());

        if (!file.exists()) {
            return 0;
        }

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
        File oldFile = new File(currentFilePath());
        fileNumber++;
        File nextFile = new File(currentFilePath());

        try {
            DataOutputStream stream = new DataOutputStream(new FileOutputStream(nextFile, false));
            stream.writeLong(number);
            stream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (oldFile.exists() && !oldFile.delete()) {
            throw new RuntimeException("Unnable to remove file: " + oldFile.getPath());
        }
    }

    private String currentFilePath() {
        return new File(directoryPath, filePrefix + "." + fileNumber).getAbsolutePath();
    }

    private int getLastFileNumber(String[] files) {
        Pattern pattern = Pattern.compile(filePrefix + "\\.(\\d+)");
        int last = -1;
        for (String fileName : files) {
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.find()) {
                int x = Integer.parseInt(matcher.group(1));
                last = Math.max(x, last);
            }
        }
        return last;
    }
}
