import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class averageFromRanges {

    private static class DataPoint {
        private List<Double> values = new ArrayList<Double>();

        public double mean() {
            double sum = 0;
            for (Double value : values)
                sum += value;
            return sum / values.size();
        }

        public void add(Double point) {
            values.add(point);
        }
    }

    private static TreeMap<Long, List<DataPoint>> times = new TreeMap<Long, List<DataPoint>>();

    public static void main(String[] args) throws Throwable {

        Locale.setDefault(Locale.ENGLISH);

        for (String arg : args) {
            BufferedReader br = new BufferedReader(new FileReader(arg));
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                String[] tokens = line.split(", *");
                if (!tokens[0].matches("^[0-9][0-9]*$"))
                    continue;
                Long time = Long.parseLong(tokens[0]);
                ArrayList<DataPoint> list = new ArrayList<DataPoint>();
                for (int i = 1; i < tokens.length; ++i)
                    list.add(new DataPoint());
                times.put(time, list);
            }
            br.close();

        }

        for (String arg : args) {
            Long prev = 0L;
            BufferedReader br = new BufferedReader(new FileReader(arg));
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                line = line.trim();
                String[] tokens = line.split(",");
                if (!tokens[0].trim().matches("^[0-9][0-9]*$")) {
                    continue;
                }
                Long time = Long.parseLong(tokens[0].trim());

                SortedMap<Long, List<DataPoint>> affected = times.subMap(prev + 1, time + 1);
                for (List<DataPoint> points : affected.values()) {
                    for (int i = 1; i < tokens.length; ++i) {
                        points.get(i - 1).add(Double.parseDouble(tokens[i].trim()));
                    }
                }
                prev = time;
            }
            br.close();
        }

        for (Entry<Long, List<DataPoint>> entry : times.entrySet()) {
            System.out.printf("%d", entry.getKey());
            for (DataPoint point : entry.getValue()) {
                System.out.printf(",%f", point.mean());
            }
            System.out.println();
        }
    }
}
