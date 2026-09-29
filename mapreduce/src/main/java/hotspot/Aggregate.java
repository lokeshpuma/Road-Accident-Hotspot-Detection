package hotspot;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input : cell|band \t score \t lat \t lon \t severity
 * Output: cell|band \t score \t count \t centLat \t centLon \t fatal \t serious \t slight
 * Intermediate value format: score,count,sumLat,sumLon,fatal,serious,slight
 */
public class Aggregate {

    static String fmt(double s, long n, double la, double lo, long f, long se, long sl) {
        return s + "," + n + "," + la + "," + lo + "," + f + "," + se + "," + sl;
    }

    static final class Sum {
        double score, lat, lon;
        long n, fatal, serious, slight;

        void add(String v) {
            String[] p = v.split(",");
            score += Double.parseDouble(p[0]);
            n += Long.parseLong(p[1]);
            lat += Double.parseDouble(p[2]);
            lon += Double.parseDouble(p[3]);
            fatal += Long.parseLong(p[4]);
            serious += Long.parseLong(p[5]);
            slight += Long.parseLong(p[6]);
        }
    }

    public static class M extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text(), v = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 5) return;
            int sev = Integer.parseInt(f[4]);
            k.set(f[0]);
            v.set(fmt(Double.parseDouble(f[1]), 1, Double.parseDouble(f[2]), Double.parseDouble(f[3]),
                    sev == 1 ? 1 : 0, sev == 2 ? 1 : 0, sev >= 3 ? 1 : 0));
            ctx.write(k, v);
        }
    }

    public static class C extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            Sum s = new Sum();
            for (Text t : vals) s.add(t.toString());
            ctx.write(key, new Text(fmt(s.score, s.n, s.lat, s.lon, s.fatal, s.serious, s.slight)));
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            Sum s = new Sum();
            for (Text t : vals) s.add(t.toString());
            ctx.write(key, new Text(String.format(Locale.ROOT, "%.4f\t%d\t%.6f\t%.6f\t%d\t%d\t%d",
                    s.score, s.n, s.lat / s.n, s.lon / s.n, s.fatal, s.serious, s.slight)));
        }
    }
}
