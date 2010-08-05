package put.consensus;

import java.io.IOException;
import java.util.Scanner;

import put.consensus.listeners.ConsensusListener;

import lsr.common.Configuration;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;

class ConsensusListenerImpl implements ConsensusListener {
	public void decide(Object obj) {
		System.out.println("Decided: " + obj);
	}
}

public class UsageTestAndExample {

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

		PaxosConsensus consensus = new PaxosConsensus(new Configuration(),
				localId, null);
		ConsensusListener cl = new ConsensusListenerImpl();
		consensus.addConsensusListener(cl);
		consensus.start();

		while (true) {
			System.out.println("Hello!\n" + "  1) Propose sth\n"
					+ "  2) Record in log\n" + "  3) Retrive from log\n"
					+ "  4) Get the value of an instance\n"
					+ "  5) Get instances count\n" + "  0) Exit\n" + "    :");
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
				ConsensusStateAndValue inst = consensus.instanceValue(id);
				if (inst == null) {
					System.out.println("No such instance");
					break;
				}
				System.out.println("State: " + inst.state);
				if (inst.state != LogEntryState.UNKNOWN)
					System.out.println("Value: " + inst.value);
				break;
			case 5:
				System.out.println("Highest InstanceID: "
						+ consensus.highestInstance());
				break;
			case 0:
				System.exit(0);
			}
		}
	}
}
