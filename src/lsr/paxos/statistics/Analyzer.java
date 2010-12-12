package lsr.paxos.statistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

class Analyzer {

    static class Interval implements Comparable<Interval> {
        public final long start;
        public long end;

        public Interval(long firstStart) {
            this.start = firstStart;
        }

        public int compareTo(Interval o) {
            long thisVal = this.start;
            long anotherVal = o.start;
            return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
        }

        public long getDuration() {
            return end - start;
        }

        public static String getHeader() {
            return "Start\tDuration\t#Req\tSize\tRetransmits\tAlpha";
        }

    }

    static final class Instance extends Interval {
        /** Number of requests ordered on this instance/batch */
        public final int nRequests;
        public final int valueSize;
        public final int alpha;
        public int retransmit = 0;

        public Instance(long firstStart, int valueSize, int nRequests, int alpha) {
            super(firstStart);
            this.nRequests = nRequests;
            this.valueSize = valueSize;
            this.alpha = alpha;
        }

        @Override
        public String toString() {
            return start / 1000 + "\t" + getDuration() / 1000 + "\t" + nRequests + "\t" +
                   valueSize + "\t" + retransmit + "\t" + alpha;
        }
    }

    public static void printList(String[] header, Map<?, ? extends Interval> list, String outFile)
            throws IOException {
        Writer ps = new BufferedWriter(new FileWriter(new File(outFile)));
        for (String s : header) {
            ps.write("%" + s + "\n");
        }
        for (Object r : list.keySet()) {
            ps.write(r + "\t" + list.get(r) + "\n");
        }
        ps.close();
    }
}
