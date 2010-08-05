package lsr.paxos.storage;

import java.io.File;
import java.io.IOException;

import lsr.paxos.storage.DiscWriter;
import lsr.paxos.storage.FullSSDiscWriter;
import lsr.paxos.storage.NioFullSSDiscWriter;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "perfomance" })
public class DiscWriterPerformanceTest {
	private String _directoryPath = "bin/logs";
	private File _directory;

	@BeforeMethod
	public void setUp() {
		_directory = new File(_directoryPath);
		deleteDir(_directory);
		_directory.mkdirs();
	}

	@AfterMethod
	public void tearDown() {
		if (!deleteDir(_directory)) {
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

	public void fullSS() throws IOException {
		FullSSDiscWriter discWriter = new FullSSDiscWriter(_directoryPath);
		long time = start(discWriter);
		discWriter.close();
		System.out.println("FullSS: " + time);
	}

	public void nioFullSS() throws IOException {
		NioFullSSDiscWriter discWriter = new NioFullSSDiscWriter(_directoryPath);
		long time = start(discWriter);
		discWriter.close();
		System.out.println("NioFullSS:" + time);
	}

	private long start(DiscWriter writer) {
		long startTime = System.currentTimeMillis();
		byte[] value = new byte[32024];
		for (int i = 0; i < 100000; i++) {
			writer.changeInstanceValue(i, i, value);
		}
		long endTime = System.currentTimeMillis();
		return endTime - startTime;
	}
}
