package hotspot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** Input line: cell|band \t score \t ...   Output: rank \t <input line> */
public class TopN {

    static final class Rec implements Comparable<Rec> {
        final double score;
        final String line;

        Rec(String line) {
            this.line = line;
            this.score = Double.parseDouble(line.split("\t")[1]);
        }

        @Override
        public int compareTo(Rec o) { // score asc, then line asc => deterministic ties
            int c = Double.compare(score, o.score);
            return c != 0 ? c : line.compareTo(o.line);
        }
    }

    static void offer(PriorityQueue<Rec> pq, Rec r, int n) {
        pq.add(r);
        if (pq.size() > n) pq.poll(); // drop the smallest
    }

    public static class M extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final PriorityQueue<Rec> pq = new PriorityQueue<>();
        private int n;

        @Override
        protected void setup(Context ctx) { n = ctx.getConfiguration().getInt(Hs.TOPN, 20); }

        @Override
        protected void map(LongWritable off, Text line, Context ctx) {
            offer(pq, new Rec(line.toString()), n);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            for (Rec r : pq) ctx.write(NullWritable.get(), new Text(r.line));
        }
    }

    public static class R extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            int n = ctx.getConfiguration().getInt(Hs.TOPN, 20);
            PriorityQueue<Rec> pq = new PriorityQueue<>();
            for (Text t : vals) offer(pq, new Rec(t.toString()), n);
            List<Rec> sorted = new ArrayList<>(pq);
            Collections.sort(sorted, Collections.reverseOrder());
            int rank = 1;
            for (Rec r : sorted) {
                ctx.write(NullWritable.get(), new Text(rank++ + "\t" + r.line));
            }
        }
    }
}
