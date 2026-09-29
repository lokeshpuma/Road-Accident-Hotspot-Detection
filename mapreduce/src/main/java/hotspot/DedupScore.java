package hotspot;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input : id \t date(ISO) \t HH:MM \t lat \t lon \t severity \t casualties
 * Output: cell|band \t score \t lat \t lon \t severity      (one line per unique collision)
 */
public class DedupScore {

    public static class M extends Mapper<LongWritable, Text, Text, Text> {
        private LocalDate asof;
        private int window;
        private double cell, halfLife;
        private final Text k = new Text(), v = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration c = ctx.getConfiguration();
            asof = LocalDate.parse(c.get(Hs.ASOF));
            window = c.getInt(Hs.WINDOW, 90);
            cell = c.getDouble(Hs.CELL, 0.02);
            halfLife = c.getDouble(Hs.HALF, 30.0);
        }

        @Override
        protected void map(LongWritable off, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 7) {
                ctx.getCounter("hotspot", "malformed").increment(1);
                return;
            }
            try {
                long age = ChronoUnit.DAYS.between(LocalDate.parse(f[1]), asof);
                if (age < 0 || age >= window) {
                    ctx.getCounter("hotspot", "outside_window").increment(1);
                    return;
                }
                int hour = Integer.parseInt(f[2].substring(0, 2));
                double lat = Double.parseDouble(f[3]);
                double lon = Double.parseDouble(f[4]);
                int sev = Integer.parseInt(f[5]);

                long li = (long) Math.floor(lat / cell);
                long oi = (long) Math.floor(lon / cell);
                double score = Hs.severityWeight(sev) * Math.pow(0.5, age / halfLife);

                k.set(f[0]); // collision_index -> duplicates meet in the same reducer call
                v.set(li + ":" + oi + "|" + Hs.band(hour) + "\t" + score + "\t" + lat + "\t" + lon + "\t" + sev);
                ctx.write(k, v);
            } catch (RuntimeException e) { // bad date / number / time
                ctx.getCounter("hotspot", "malformed").increment(1);
            }
        }
    }

    public static class R extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text id, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            boolean first = true;
            for (Text t : vals) {
                if (first) {
                    ctx.write(NullWritable.get(), t);
                    first = false;
                } else {
                    ctx.getCounter("hotspot", "duplicates").increment(1);
                }
            }
        }
    }
}
