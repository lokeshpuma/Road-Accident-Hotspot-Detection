package hotspot;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Usage: hadoop jar hotspot-mr-1.0.jar hotspot.HotspotDriver <asof yyyy-MM-dd> <baseDir> [windowDays=90] [topN=20]
 * Example: ... HotspotDriver 2025-12-31 /data/stats19 90 20
 */
public class HotspotDriver {

    public static void main(String[] a) throws Exception {
        if (a.length < 2) {
            System.err.println("usage: HotspotDriver <asof yyyy-MM-dd> <baseDir> [windowDays] [topN]");
            System.exit(1);
        }
        LocalDate asof = LocalDate.parse(a[0]);
        String base = a[1];
        int window = a.length > 2 ? Integer.parseInt(a[2]) : 90;
        int topN = a.length > 3 ? Integer.parseInt(a[3]) : 20;

        Configuration conf = new Configuration();
        conf.set(Hs.ASOF, asof.toString());
        conf.setInt(Hs.WINDOW, window);
        conf.setInt(Hs.TOPN, topN);
        FileSystem fs = FileSystem.get(conf);

        // 1. Rolling window = the `window` most recent dt= folders ending at asof
        List<Path> inputs = new ArrayList<>();
        for (int i = 0; i < window; i++) {
            Path p = new Path(base + "/raw/dt=" + asof.minusDays(i));
            if (fs.exists(p)) inputs.add(p);
        }
        if (inputs.isEmpty()) {
            System.err.println("No input folders found for window ending " + asof);
            System.exit(2);
        }
        if (inputs.size() < window) {
            System.err.println("WARN: only " + inputs.size() + "/" + window + " day-folders present");
        }

        Path tmp = new Path(base + "/tmp/asof=" + asof);
        Path out = new Path(base + "/output/asof=" + asof);
        fs.delete(tmp, true); // idempotent re-run
        fs.delete(out, true);
        Path scored = new Path(tmp, "scored");
        Path cells = new Path(out, "cells");
        Path top = new Path(out, "top20");

        // Job 1: filter window, de-duplicate, score
        Job j1 = Job.getInstance(conf, "hotspot-1-dedup-score-" + asof);
        j1.setJarByClass(HotspotDriver.class);
        j1.setMapperClass(DedupScore.M.class);
        j1.setReducerClass(DedupScore.R.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(Text.class);
        j1.setOutputKeyClass(NullWritable.class);
        j1.setOutputValueClass(Text.class);
        j1.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(j1, 16 * 1024 * 1024);
        j1.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(j1, inputs.toArray(new Path[0]));
        FileOutputFormat.setOutputPath(j1, scored);
        if (!j1.waitForCompletion(true)) System.exit(3);

        // Job 2: aggregate by (cell, band)
        Job j2 = Job.getInstance(conf, "hotspot-2-aggregate-" + asof);
        j2.setJarByClass(HotspotDriver.class);
        j2.setMapperClass(Aggregate.M.class);
        j2.setCombinerClass(Aggregate.C.class);
        j2.setReducerClass(Aggregate.R.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        j2.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(j2, 16 * 1024 * 1024);
        j2.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(j2, scored);
        FileOutputFormat.setOutputPath(j2, cells);
        if (!j2.waitForCompletion(true)) System.exit(4);

        // Job 3: global top-N
        Job j3 = Job.getInstance(conf, "hotspot-3-topn-" + asof);
        j3.setJarByClass(HotspotDriver.class);
        j3.setMapperClass(TopN.M.class);
        j3.setReducerClass(TopN.R.class);
        j3.setOutputKeyClass(NullWritable.class);
        j3.setOutputValueClass(Text.class);
        j3.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(j3, cells);
        FileOutputFormat.setOutputPath(j3, top);
        boolean ok = j3.waitForCompletion(true);

        fs.delete(tmp, true);
        System.exit(ok ? 0 : 5);
    }
}
