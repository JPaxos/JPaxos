package lsr.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

@Test(groups = { "unit" })
public class PrimitivesByteArrayTest {
	private int[] integers = new int[] { Integer.MIN_VALUE, -1367309237, -23463, -65535, -65536, -256, -255, -1, 0, 1,
			128, 255, 256, 65535, 65536, 32312343, 1000000000, Integer.MAX_VALUE };

	public void testIntegerToBytesAndFromBytes() {
		for (int i : integers) {
			byte[] bytes = PrimitivesByteArray.fromInt(i);
			assertEquals(4, bytes.length);
			int x = PrimitivesByteArray.toInt(bytes);
			assertEquals(i, x);
		}
	}

	public void testIntegerToStreamAndFromStream() throws IOException {
		ByteArrayOutputStream os;
		ByteArrayInputStream is;
		for (int i : integers) {
			os = new ByteArrayOutputStream();
			PrimitivesByteArray.fromInt(i, os);
			assertEquals(4, os.toByteArray().length);
			is = new ByteArrayInputStream(os.toByteArray());
			int x = PrimitivesByteArray.toInt(is);
			assertEquals(0, is.available());
			assertEquals(i, x);
		}
	}

	public void testIntegerToStreamAndFromBytes() throws IOException {
		ByteArrayOutputStream os;
		for (int i : integers) {
			os = new ByteArrayOutputStream();
			PrimitivesByteArray.fromInt(i, os);
			assertEquals(4, os.toByteArray().length);
			int x = PrimitivesByteArray.toInt(os.toByteArray());
			assertEquals(i, x);
		}
	}

	public void testIntegerToBytesAndFromStream() throws IOException {
		ByteArrayInputStream is;
		for (int i : integers) {
			byte[] bytes = PrimitivesByteArray.fromInt(i);
			assertEquals(4, bytes.length);
			is = new ByteArrayInputStream(bytes);
			int x = PrimitivesByteArray.toInt(is);
			assertEquals(0, is.available());
			assertEquals(i, x);
		}
	}

	private long[] longs = new long[] { Long.MIN_VALUE, -919237074714678956L, Integer.MIN_VALUE, -1367309237, -23463,
			-65535, -65536, -256, -255, -1, 0, 1, 128, 255, 256, 65535, 65536, 32312343, 1000000000,
			234858778534246778L, Integer.MAX_VALUE, Long.MAX_VALUE };

	public void testLongToBytesAndFromBytes() {
		for (long i : longs) {
			byte[] bytes = PrimitivesByteArray.fromLong(i);
			assertEquals(8, bytes.length);
			long x = PrimitivesByteArray.toLong(bytes);
			assertEquals(i, x);
		}
	}

	public void testLongToStreamAndFromStream() throws IOException {
		ByteArrayOutputStream os;
		ByteArrayInputStream is;
		for (long i : longs) {
			os = new ByteArrayOutputStream();
			PrimitivesByteArray.fromLong(i, os);
			assertEquals(8, os.toByteArray().length);
			is = new ByteArrayInputStream(os.toByteArray());
			long x = PrimitivesByteArray.toLong(is);
			assertEquals(0, is.available());
			assertEquals(i, x);
		}
	}

	public void testLongToStreamAndFromBytes() throws IOException {
		ByteArrayOutputStream os;
		for (long i : longs) {
			os = new ByteArrayOutputStream();
			PrimitivesByteArray.fromLong(i, os);
			assertEquals(8, os.toByteArray().length);
			long x = PrimitivesByteArray.toLong(os.toByteArray());
			assertEquals(i, x);
		}
	}

	public void testLongToBytesAndFromStream() throws IOException {
		ByteArrayInputStream is;
		for (long i : longs) {
			byte[] bytes = PrimitivesByteArray.fromLong(i);
			assertEquals(8, bytes.length);
			is = new ByteArrayInputStream(bytes);
			long x = PrimitivesByteArray.toLong(is);
			assertEquals(0, is.available());
			assertEquals(i, x);
		}
	}
}
