package serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Vector;

import javax.swing.JOptionPane;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

public class FromFileSerialization {

	public static void main(String args[]) throws IOException, ClassNotFoundException {

		FileInputStream fis;
		try {
			fis = new FileInputStream(JOptionPane.showInputDialog("Filename..."));
		} catch (NullPointerException e) {
			System.exit(1);
			return;
		}

		DataInputStream dis = new DataInputStream(fis);
		List<Message> msgs = new Vector<Message>();
		while (fis.available() != 0) {
			dis.readInt();
			msgs.add(MessageFactory.create(dis));
		}

		fis.close();

		System.out.println("Message count: " + msgs.size());

		List<byte[]> our = new Vector<byte[]>();
		List<byte[]> java = new Vector<byte[]>();
		List<Message> ourM = new Vector<Message>();
		List<Message> javaM = new Vector<Message>();
		byte[] byteArray;

		long lengthsum = 0;

		long start = System.nanoTime();
		for (Message ms : msgs) {
			byteArray = ms.toByteArray();
			our.add(byteArray);
			lengthsum += byteArray.length;
		}
		long stop = System.nanoTime();

		System.out.println("Our____serialization_time " + (((double) (stop - start)) / 1e9) + " _avg_size "
				+ (((double) lengthsum) / msgs.size()));

		start = System.nanoTime();
		for (byte[] ms : our) {
			ByteArrayInputStream bais = new ByteArrayInputStream(ms);
			ourM.add(MessageFactory.create(new DataInputStream(bais)));
		}
		stop = System.nanoTime();

		System.out.println("Our__deserialization_time " + (((double) (stop - start)) / 1e9));

		lengthsum = 0;

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		start = System.nanoTime();
		for (Message ms : msgs) {
			baos.reset();
			new ObjectOutputStream(baos).writeObject(ms);
			byteArray = baos.toByteArray();
			java.add(byteArray);
			lengthsum += byteArray.length;
		}
		stop = System.nanoTime();

		System.out.println("Java___serialization_time " + (((double) (stop - start)) / 1e9) + " _avg_size "
				+ (((double) lengthsum) / msgs.size()));

		lengthsum = 0;

		ByteArrayInputStream bais;
		start = System.nanoTime();
		for (byte[] ms : java) {
			bais = new ByteArrayInputStream(ms);
			javaM.add((Message) new ObjectInputStream(bais).readObject());
		}
		stop = System.nanoTime();

		System.out.println("Java_deserialization_time " + (((double) (stop - start)) / 1e9));

	}

}



//Message count: 1079429
//Our____serialization_time 0.909168376 _avg_size 166.71224693796444
//Our__deserialization_time 1.054995782
//Java___serialization_time 7.002709321 _avg_size 320.3365408933797
//Java_deserialization_time 21.966526144
