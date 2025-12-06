import java.io.IOException;
import java.io.BufferedInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeightedHeavyRead {

  public static enum HeavyCounter {
    BYTES_READ
  }

  public static class HeavyMapper
      extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private FileSystem fs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      fs = FileSystem.get(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString().trim();
      if (line.isEmpty()) {
        return;
      }

      // Format générique: path, ..., nreads quelque part après la première colonne
      String[] parts = line.split(",");
      if (parts.length < 2) {
        return;
      }

      String pathStr = parts[0].trim();

      // Chercher la première colonne numérique > 0 comme nreads
      int nreads = 0;
      for (int i = 1; i < parts.length; i++) {
        String c = parts[i].trim();
        if (c.isEmpty()) {
          continue;
        }
        try {
          int v = Integer.parseInt(c);
          if (v > 0) {
            nreads = v;
            break;
          }
        } catch (NumberFormatException e) {
          // ignorer, ce n'est pas un entier
        }
      }

      // Si on n'a rien trouvé, on ne lit pas le fichier
      if (nreads <= 0) {
        return;
      }

      Path p = new Path(pathStr);
      long bytesReadTotal = 0L;

      for (int i = 0; i < nreads; i++) {
        try (FSDataInputStream in = fs.open(p);
             BufferedInputStream bin = new BufferedInputStream(in)) {

          byte[] buffer = new byte[64 * 1024];
          int read;
          while ((read = bin.read(buffer)) != -1) {
            bytesReadTotal += read;
          }
        }
      }

      context.getCounter(HeavyCounter.BYTES_READ).increment(bytesReadTotal);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WeightedHeavyRead <weights.csv> <outDir>");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "weighted word count heavy read");

    job.setJarByClass(WeightedHeavyRead.class);
    job.setMapperClass(HeavyMapper.class);

    job.setNumReduceTasks(0);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.setNumLinesPerSplit(job, 50);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    boolean ok = job.waitForCompletion(true);
    if (!ok) {
      System.exit(1);
    }

    long bytes = job.getCounters()
                    .findCounter(HeavyCounter.BYTES_READ)
                    .getValue();

    System.out.println("HEAVY BYTES_READ = " + bytes);
    System.exit(0);
  }
}
