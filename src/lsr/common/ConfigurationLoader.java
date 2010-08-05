package lsr.common;

import java.io.IOException;
import java.util.List;

/**
 * Provides public interface for classes related with loading replicas
 * configuration.
 */
public interface ConfigurationLoader {
	/**
	 * Returns configuration with information about replicas.
	 * 
	 * @return list of replica informations
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	List<PID> load() throws IOException;
}
