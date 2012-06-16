package lsr.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Holds the configuration of the system. It consists of
 * <ul>
 * <li>the list of replicas {@link PID}
 * <li>an optional list of configuration properties.
 * </ul>
 * 
 * <p>
 * The configuration file is a standard java properties file (readable with the
 * class {@link Properties}). The PIDs of the replicas must be specified as in
 * the example below:
 * 
 * <pre>
 * # Mandatory properties
 * process.0 = &lt;host0&gt;:&lt;clientPort0&gt;:&lt;replicaPort0&gt;
 * process.1 = &lt;host1&gt;:&lt;clientPort1&gt;:&lt;replicaPort1&gt;
 * process.2 = &lt;host2&gt;:&lt;clientPort2&gt;:&lt;replicaPort2&gt;
 * 
 * # Optional properties
 * propkey = propvalue
 * ...
 * </pre>
 * 
 * It's possible to specify any number of replicas, as long as they are numbered
 * sequentially starting at 0, in the form of <code>process.x</code>.
 * 
 * @author Nuno Santos (LSR)
 * 
 */
public final class Configuration {
    /*---------------------------------------------
     * The following properties are compile time constants.
     *---------------------------------------------*/
    public static final int UDP_RECEIVE_BUFFER_SIZE = 64 * 1024;
    public static final int UDP_SEND_BUFFER_SIZE = 64 * 1024;
    /** for re-sending catch-up query we use a separate, self-adjusting timeout */
    public static final long CATCHUP_MIN_RESEND_TIMEOUT = 50;

    private final List<PID> processes;

    private final Properties configuration = new Properties();

    /**
     * Loads the configuration from the default file
     * <code>paxos.properties</code>
     * 
     * @throws IOException
     */
    public Configuration() throws IOException {
        this("paxos.properties");
    }

    /**
     * Loads the configuration from the given file.
     * 
     * @param confFile
     * @throws IOException
     */
    public Configuration(String confFile) throws IOException {
        // Load property from file there is one
        FileInputStream fis = new FileInputStream(confFile);
        configuration.load(fis);
        fis.close();
        logger.info("Configuration loaded from file: " + confFile);

        this.processes = Collections.unmodifiableList(loadProcessList());
    }

    /**
     * Creates a configuration with the process list, and an empty set of
     * optional properties.
     * 
     * @param processes
     */
    public Configuration(List<PID> processes) {
        this.processes = processes;
    }

    public int getN() {
        return processes.size();
    }

    public List<PID> getProcesses() {
        return processes;
    }

    public PID getProcess(int id) {
        return processes.get(id);
    }

    public boolean containsKey(String key) {
        return configuration.containsKey(key);
    }

    /**
     * Returns a given property, converting the value to an integer.
     * 
     * @param key - the key identifying the property
     * @param defValue - the default value to use in case the key is not found.
     * @return the value of key property or defValue if key not found
     */
    public int getIntProperty(String key, int defValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.fine("Property not found: " + key + ". Using default value: " +
                        defValue);
            return defValue;
        }
        return Integer.parseInt(str);
    }

    /**
     * Returns a given property, converting the value to a boolean.
     * 
     * @param key - the key identifying the property
     * @param defValue - the default value to use in case the key is not found.
     * @return the value of key property or defValue if key not found
     */
    public boolean getBooleanProperty(String key, boolean defValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.fine("Property not found: " + key + ". Using default value: " + defValue);
            return defValue;
        }
        return Boolean.parseBoolean(str);
    }

    /**
     * 
     * @param key - the key identifying the property
     * @param defValue - the default value to use in case the key is not found.
     * 
     * @return the value of key property or defValue if key not found
     */
    public String getProperty(String key, String defValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.fine("Property not found: " + key + ". Using default value: " + defValue);
            return defValue;
        }
        return str;
    }

    private List<PID> loadProcessList() {
        List<PID> processes = new ArrayList<PID>();
        int i = 0;
        while (true) {
            String line = configuration.getProperty("process." + i);
            if (line == null) {
                break;
            }
            StringTokenizer st = new StringTokenizer(line, ":");
            PID pid = new PID(i, st.nextToken(), Integer.parseInt(st.nextToken()),
                    Integer.parseInt(st.nextToken()));
            processes.add(pid);
            logger.info(pid.toString());
            i++;
        }
        return processes;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Processes:\n");
        for (PID p : processes) {
            sb.append("  ").append(p).append("\n");
        }
        sb.append("Properties:\n");
        Object[] keys = configuration.keySet().toArray();
        Arrays.sort(keys);
        for (Object key : keys) {
            sb.append("  ").append(key).append("=").append(configuration.get(key)).append("\n");
        }
        // Remove the trailing '\n'
        return sb.substring(0, sb.length() - 1);
    }

    public double getDoubleProperty(String key, double defultValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.fine("Property not found: " + key + ". Using default value: " + defultValue);
            return defultValue;
        }
        return Double.parseDouble(str);
    }

    public long getLongProperty(String key, long defultValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.fine("Property not found: " + key + ". Using default value: " + defultValue);
            return defultValue;
        }
        return Long.parseLong(str);
    }

    private final static Logger logger = Logger.getLogger(Configuration.class.getCanonicalName());
}
