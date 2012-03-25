package lsr.common;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

public class ConfigurationTest {
    @Test
    public void shouldInitializeProcesses() {
        List<PID> processes = new ArrayList<PID>();
        processes.add(new PID(0, "localhost", 2000, 3000));
        processes.add(new PID(1, "localhost", 2001, 3001));
        processes.add(new PID(2, "localhost", 2002, 3002));

        Configuration configuration = new Configuration(processes);
        assertEquals(3, configuration.getN());
        assertEquals(processes, configuration.getProcesses());
        assertEquals(processes.get(0), configuration.getProcess(0));
    }

    @Test
    public void shouldReturnDefaultInt() {
        Configuration configuration = new Configuration((List<PID>) null);
        assertEquals(10, configuration.getIntProperty("test", 10));
        assertEquals(15, configuration.getIntProperty("test", 15));
    }

    @Test
    public void shouldReturnDefaultBoolean() {
        Configuration configuration = new Configuration((List<PID>) null);
        assertEquals(true, configuration.getBooleanProperty("test", true));
        assertEquals(false, configuration.getBooleanProperty("test", false));
    }

    @Test
    public void shouldReturnDefaultString() {
        Configuration configuration = new Configuration((List<PID>) null);
        assertEquals("my", configuration.getProperty("test", "my"));
    }

    @Test
    public void shouldBePrintable() {
        List<PID> processes = new ArrayList<PID>();
        processes.add(new PID(0, "localhost", 2000, 3000));
        processes.add(new PID(1, "localhost", 2001, 3001));
        processes.add(new PID(2, "localhost", 2002, 3002));

        Configuration configuration = new Configuration(processes);

        configuration.toString();
    }

    @Test
    public void shouldLoadFromFile() throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.put("process.0", "localhost:2000:3000");
        properties.put("process.1", "localhost:2001:3001");
        properties.put("process.2", "localhost:2002:3002");
        properties.put("integer", "5");
        properties.put("boolean", "true");
        properties.put("string", "hello world");

        File tempFile = File.createTempFile("paxos", "");

        FileOutputStream outputStream = new FileOutputStream(tempFile);
        properties.store(outputStream, "");
        outputStream.close();

        Configuration configuration = new Configuration(tempFile.getAbsolutePath());
        assertEquals(3, configuration.getN());
        assertEquals("localhost", configuration.getProcess(0).getHostname());
        assertEquals(2000, configuration.getProcess(0).getReplicaPort());
        assertEquals(3000, configuration.getProcess(0).getClientPort());

        assertEquals(5, configuration.getIntProperty("integer", 3));
        assertEquals(true, configuration.getBooleanProperty("boolean", false));
        assertEquals("hello world", configuration.getProperty("string", "default"));
    }
}
