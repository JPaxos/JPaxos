package lsr.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Converts some primitive types to and from byte [] and/or input/output stream
 * using big-endian byte storing convention.
 * 
 * As for now supported:
 * <ul>
 * <li>int</li>
 * <li>long</li>
 * </ul>
 * 
 */
public class PrimitivesByteArray {
	public static byte[] fromInt(int i) {
		byte[] b = new byte[4];
		b[0] = (byte) (i);
		i >>= 8;
		b[1] = (byte) (i);
		i >>= 8;
		b[2] = (byte) (i);
		i >>= 8;
		b[3] = (byte) (i);
		return b;
	}

	public static void fromInt(int i, OutputStream b) throws IOException {
		b.write((byte) (i));
		i >>= 8;
		b.write((byte) (i));
		i >>= 8;
		b.write((byte) (i));
		i >>= 8;
		b.write((byte) (i));
	}

	public static int toInt(byte[] b) {
		int i = (b[0] & 0xFF);
		i |= (b[1] & 0xFF) << 8;
		i |= (b[2] & 0xFF) << 16;
		i |= (b[3] & 0xFF) << 24;
		return i;
	}

	public static int toInt(InputStream b) throws IOException {
		int i = b.read();
		i |= b.read() << 8;
		i |= b.read() << 16;
		i |= b.read() << 24;
		return i;
	}

	public static byte[] fromLong(long l) {
		byte[] b = new byte[8];
		b[0] = (byte) (l);
		l >>= 8;
		b[1] = (byte) (l);
		l >>= 8;
		b[2] = (byte) (l);
		l >>= 8;
		b[3] = (byte) (l);
		l >>= 8;
		b[4] = (byte) (l);
		l >>= 8;
		b[5] = (byte) (l);
		l >>= 8;
		b[6] = (byte) (l);
		l >>= 8;
		b[7] = (byte) (l);
		return b;
	}

	public static void fromLong(long l, OutputStream b) throws IOException {
		b.write((byte) (l));
		l >>= 8;
		b.write((byte) (l));// 2
		l >>= 8;
		b.write((byte) (l));
		l >>= 8;
		b.write((byte) (l));// 4
		l >>= 8;
		b.write((byte) (l));
		l >>= 8;
		b.write((byte) (l));// 6
		l >>= 8;
		b.write((byte) (l));
		l >>= 8;
		b.write((byte) (l));
	}

	public static long toLong(byte[] b) {
		long l = b[0] & 0xFF;
		l |= ((long) b[1] & 0xFF) << 8;
		l |= ((long) b[2] & 0xFF) << 8 * 2;
		l |= ((long) b[3] & 0xFF) << 8 * 3;
		l |= ((long) b[4] & 0xFF) << 8 * 4;
		l |= ((long) b[5] & 0xFF) << 8 * 5;
		l |= ((long) b[6] & 0xFF) << 8 * 6;
		l |= ((long) b[7] & 0xFF) << 8 * 7;
		return l;
	}

	public static long toLong(InputStream b) throws IOException {
		long l = b.read();
		l |= ((long) b.read()) << 8;

		l |= ((long) b.read()) << 8 * 2;
		l |= ((long) b.read()) << 8 * 3;

		l |= ((long) b.read()) << 8 * 4;
		l |= ((long) b.read()) << 8 * 5;

		l |= ((long) b.read()) << 8 * 6;
		l |= ((long) b.read()) << 8 * 7;
		return l;
	}
}
