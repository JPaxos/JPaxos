package lsr.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Reads given file and creates a process ID basing on it.
 * <p>
 * File format: hostname:replica_port:client_port
 * </p>
 * 
 * <dl>
 * <dt>hostname
 * <dd>string based
 * <dt>ports
 * <dd>numeric
 * </dl>
 */
public class FileConfigurationLoader implements ConfigurationLoader {
    public final static String DEFAULT_CONFIG = "nodes.conf";
    private final String fileName;

    /**
     * Creates loader which use default configuration file.
     * 
     * @see #DEFAULT_CONFIG
     */
    public FileConfigurationLoader() {
        this(DEFAULT_CONFIG);
    }

    /**
     * Create loader which use configuration from specified file.
     * 
     * @param fileName - path to file with configuration
     */
    public FileConfigurationLoader(String fileName) {
        this.fileName = fileName;
    }

    public List<PID> load() throws IOException {
        List<PID> processes = new ArrayList<PID>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        int i = 0;
        logger.info("Configuration");
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("#") || line.equals(""))
                continue;

            StringTokenizer st = new StringTokenizer(line, ":");
            PID pid = new PID(i, st.nextToken(), Integer.parseInt(st.nextToken()),
                    Integer.parseInt(st.nextToken()));
            processes.add(pid);
            logger.info(pid.toString());
            i++;
        }
        return processes;
    }

    private final static Logger logger = Logger.getLogger(FileConfigurationLoader.class.getCanonicalName());
}
