package lsr.paxos.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class RandomRequestGenerator {
	private final Random random;

	public RandomRequestGenerator() {
		random = new Random();
	}

	public RandomRequestGenerator(int seed) {
		random = new Random(seed);
	}

	public byte[] generate(int size, byte b) {
		byte[] value = new byte[size];
		Arrays.fill(value, b);
		return value;
	}

	public byte[] generate() {
		long key = random.nextInt(100);
		long a = random.nextInt(100) + 2;
		long b = random.nextInt(100) + 1;
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream dataStream = new DataOutputStream(byteStream);
		try {
			dataStream.writeLong(key);
			dataStream.writeLong(a);
			dataStream.writeLong(b);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return byteStream.toByteArray();
	}
}
