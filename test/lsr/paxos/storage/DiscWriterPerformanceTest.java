package lsr.paxos.storage;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class DiscWriterPerformanceTest {
	private String directoryPath = "bin/logs";
	private File directory;

	@Before
	public void setUp() {
		directory = new File(directoryPath);
		deleteDir(directory);
		directory.mkdirs();
	}

	@After
	public void tearDown() {
		if (!deleteDir(directory)) {
			throw new RuntimeException("Directory was not removed");
		}
	}

	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return dir.delete();
	}

	@Test
	public void fullSS() throws IOException {
		FullSSDiscWriter discWriter = new FullSSDiscWriter(directoryPath);
		long time = start(discWriter);
		discWriter.close();
		System.out.println("FullSS: " + time);
	}

	@Test
	@Ignore
	public void nioFullSS() throws IOException {
		NioFullSSDiscWriter discWriter = new NioFullSSDiscWriter(directoryPath);
		long time = start(discWriter);
		discWriter.close();
		System.out.println("NioFullSS:" + time);
	}

	private long start(DiscWriter writer) {
		long startTime = System.currentTimeMillis();
		byte[] value = new byte[32024];
		for (int i = 0; i < 1000; i++) {
			writer.changeInstanceValue(i, i, value);
		}
		long endTime = System.currentTimeMillis();
		return endTime - startTime;
	}
}
