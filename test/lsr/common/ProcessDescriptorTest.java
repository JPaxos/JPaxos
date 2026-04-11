package lsr.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

public class ProcessDescriptorTest {
    @Test
    public void shouldLoadDynatunePropertiesFromConfiguration() throws IOException {
        Properties properties = new Properties();
        properties.put("process.0", "localhost:2000:3000");
        properties.put("process.1", "localhost:2001:3001");
        properties.put("process.2", "localhost:2002:3002");
        properties.put(ProcessDescriptor.DYNATUNE_ENABLED, "true");
        properties.put(ProcessDescriptor.DYNATUNE_SAFETY_FACTOR, "3.5");
        properties.put(ProcessDescriptor.DYNATUNE_HEARTBEAT_PROBABILITY, "0.95");
        properties.put(ProcessDescriptor.DYNATUNE_MIN_LIST_SIZE, "12");
        properties.put(ProcessDescriptor.DYNATUNE_MAX_LIST_SIZE, "200");

        File tempFile = File.createTempFile("paxos", "");
        FileOutputStream outputStream = new FileOutputStream(tempFile);
        properties.store(outputStream, "");
        outputStream.close();

        Configuration configuration = new Configuration(tempFile.getAbsolutePath());
        ProcessDescriptor.initialize(configuration, 0);

        assertEquals(true, ProcessDescriptor.processDescriptor.dynatuneEnabled);
        assertEquals(3.5, ProcessDescriptor.processDescriptor.dynatuneSafetyFactor, 0.0);
        assertEquals(0.95,
                ProcessDescriptor.processDescriptor.dynatuneHeartbeatProbability, 0.0);
        assertEquals(12, ProcessDescriptor.processDescriptor.dynatuneMinListSize);
        assertEquals(200, ProcessDescriptor.processDescriptor.dynatuneMaxListSize);
    }

    @Test
    public void shouldUseDefaultDynatunePropertiesWhenUnset() {
        ProcessDescriptorHelper.initialize(3, 0);

        assertFalse(ProcessDescriptor.processDescriptor.dynatuneEnabled);
        assertEquals(ProcessDescriptor.DEFAULT_DYNATUNE_SAFETY_FACTOR,
                ProcessDescriptor.processDescriptor.dynatuneSafetyFactor, 0.0);
        assertEquals(ProcessDescriptor.DEFAULT_DYNATUNE_HEARTBEAT_PROBABILITY,
                ProcessDescriptor.processDescriptor.dynatuneHeartbeatProbability, 0.0);
        assertEquals(ProcessDescriptor.DEFAULT_DYNATUNE_MIN_LIST_SIZE,
                ProcessDescriptor.processDescriptor.dynatuneMinListSize);
        assertEquals(ProcessDescriptor.DEFAULT_DYNATUNE_MAX_LIST_SIZE,
                ProcessDescriptor.processDescriptor.dynatuneMaxListSize);
    }
}
