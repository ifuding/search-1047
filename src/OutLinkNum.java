/*
description: In the v0.0.1 the pageRank algorithm need the number of outLinks in every urls and
             the inital PageRankMap.
             In the v0.0.2 don't need the OutLinkNum, but also need the initial PageRankMap. 
input: linkdb_data, every single line includes the url and the urls which direct \
       to the first url.
ouput: OutLinkMap MapFile, <key, value> is <url, outLinkNum>
Mapper: <null, targetUrl and srcUrls> --> <srcUrl, 1>
Reduccer: <url, Iterator<DoubleWritable>> --> <url, OutLinkNum>
*/
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

public class OutLinkNum {
  public static class OutLinkNumMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String urls[] = line.split(" ");
      int urlSize = urls.length;
      for(int i = 0; i < urlSize; i++) {
  	context.write(new Text(urls[i]), new IntWritable(1));
      }
    }
  }
  
  public static class OutLinkNumReducer extends Reducer <Text, IntWritable, Text, DoubleWritable> {
    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      double outLinkNum = 0;
     /* for(IntWritable value : values) {
        outLinkNum++;
      }*/
      context.write(key, new DoubleWritable(1));//outLinkNum));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: OutLinks <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(OutLinks.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setOutputFormatClass(MapFileOutputFormat.class);
    job.setMapperClass(OutLinkNumMapper.class);
    job.setReducerClass(OutLinkNumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.waitForCompletion(true);
  }
}
