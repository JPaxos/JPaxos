package lsr.common;

import java.text.DecimalFormat;
import java.text.FieldPosition;

public class Util {
	/**
	 * Objects required to format data. They are expensive to create, so we keep
	 * copies of these objects. One per thread to ensure thread safety.
	 */
	static private class AuxData {
		public final DecimalFormat df = new DecimalFormat("#0.00");
		public final DecimalFormat expf = new DecimalFormat("0.0E0");
		public final FieldPosition fp = new FieldPosition(0);
	}

	private static ThreadLocal<AuxData> tl = new ThreadLocal<AuxData>() {
		protected AuxData initialValue() {
			return new AuxData();
		};
	};

	public static String toString(double[] vector) {
		StringBuffer sb = new StringBuffer();
		toString(vector, sb);
		return sb.toString();
	}

	public static String toString(double number) {
		StringBuffer sb = new StringBuffer();
		toString(number, sb);
		return sb.toString();
	}

	public static String toString(double[][] array) {
		StringBuffer sb = new StringBuffer();
		String sep = "";
		for (int i = 0; i < array.length; i++) {
			sb.append(sep);
			sep = "\n";
			toString(array[i], sb);
		}
		return sb.toString();
	}

	private static void toString(double[] vector, StringBuffer sb) {
		String sep = "";
		for (int j = 0; j < vector.length; j++) {
			sb.append(sep);
			sep = " ";
			toString(vector[j], sb);
		}
	}

	private static void toString(double number, StringBuffer sb) {
		DecimalFormat df = number > 1000 ? tl.get().expf : tl.get().df;
		df.format(number, sb, tl.get().fp);
	}
}
