package SearchPackage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

public class SearchReSort {
  public static class SearchReSortMapper extends Mapper <LongWritable, Text, DoubleWritable, Text> {
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String urls[] = line.split("\t");
      double urlWeight = Double.parseDouble(urls[1]);
      context.write(new DoubleWritable(urlWeight), new Text(urls[0]));
    }
  }
  
  public static class SearchReSortReducer extends Reducer <DoubleWritable, Text, DoubleWritable, Text > {
    public void reduce (DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for(Text value : values) {
        context.write(key, value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: SearchReSort <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(SearchReSort.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(SearchReSortMapper.class);
    job.setReducerClass(SearchReSortReducer.class);

    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);

    job.waitForCompletion(true);
  }
}
