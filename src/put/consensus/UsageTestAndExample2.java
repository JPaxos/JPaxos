package put.consensus;

import java.io.IOException;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.Map.Entry;

import lsr.common.Configuration;
import put.consensus.listeners.CommitListener;
import put.consensus.listeners.ConsensusListener;
import put.consensus.listeners.RecoveryListener;

class ListenerImpl implements CommitListener, RecoveryListener, ConsensusListener {

	private final Commitable commitable;
	private Integer number = 0;

	public ListenerImpl(Commitable c) {
		commitable = c;
	}

	public void decide(Object obj) {
		System.out.println("Decided: " + obj);
		if (Math.random() > 0.66) {
			number = number + 1;
			commitable.commit(number);
		}
		if (Math.random() > 0.9) {
			number = number + 1;
			commitable.commit(number);
		}
	}

	public void onCommit(Object commitData) {
		System.out.println("TimeJustAfterCommit: " + commitData);
	}

	public void recoverFromCommit(Object commitData) {
		System.out.println("RecoveringFromCommit: " + commitData);
		number = (Integer) commitData;
	}

	@Override
	public void recoveryFinished() {
		System.out.println("Recovering finished!");
	}
}

public class UsageTestAndExample2 {

	public static void main(String[] args) throws IOException, StorageException {

		if (args.length != 1) {
			System.err.println("Need replica number");
			System.exit(1);
		}

		int localId = 0;
		try {
			localId = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			System.err.println("Need replica number");
			System.exit(1);
		}

		SerializablePaxosConsensus consensus = new SerializablePaxosConsensus(new Configuration(), localId);
		ListenerImpl listener = new ListenerImpl(consensus);

		consensus.addConsensusListener(listener);
		consensus.addCommitListener(listener);
		consensus.addRecoveryListener(listener);

		consensus.start();

		while (true) {
			System.out.println("Hello!\n" + "  1) Propose sth\n" + "  2) Record in log\n" + "  3) Retrive from log\n"
					+ "  4) Get the n-th value\n" + "  5) Get values [a..b]\n" + "  6) Get all values\n"
					+ "  7) Get value count\n" + "  0) Exit\n" + "    :");
			Scanner sc = new Scanner(System.in);
			switch (Integer.parseInt(sc.nextLine())) {
				case 1:
					System.out.print("What: ");
					consensus.propose(sc.nextLine());
					break;
				case 2:
					System.out.print("Key: ");
					String key = sc.nextLine();
					System.out.print("Val: ");
					String val = sc.nextLine();
					consensus.log(key, val);
					break;
				case 3:
					System.out.print("Key: ");
					System.out.println(consensus.retrieve(sc.nextLine()));
					break;
				case 4:
					System.out.print("ID: ");
					int id = Integer.parseInt(sc.nextLine());
					Object inst = consensus.getRequest(id);
					if (inst == null) {
						System.out.println("No such instance");
						break;
					}
					System.out.println("Value: " + inst);
					break;
				case 5:
					System.out.print("Start: ");
					int start = Integer.parseInt(sc.nextLine());
					System.out.print("Stop: ");
					int stop = Integer.parseInt(sc.nextLine());
					SortedMap<Integer, Object> map = consensus.getRequests(start, stop);
					for (Entry<Integer, Object> o : map.entrySet()) {
						System.out.println(o.getKey() + ": " + o.getValue());
					}
					System.out.println("Total: " + map.size());
					break;
				case 6:
					SortedMap<Integer, Object> fullMap = consensus.getRequests();
					for (Entry<Integer, Object> o : fullMap.entrySet()) {
						System.out.println(o.getKey() + ": " + o.getValue());
					}
					System.out.println("Total: " + fullMap.size());
					break;
				case 7:
					System.out.println("Highest requestID: " + consensus.getHighestExecuteSeqNo());
					break;
				case 0:
					System.exit(0);
			}
		}
	}
}
