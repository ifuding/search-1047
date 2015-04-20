/*
description: The linkdb output by nutch contain the url and the urls \ 
             that direct to the first url in every single line.Then we \
             need to convert it to the url and the urls that the url direct \
             to in ervery single line.
input: linkdb_data Text.
output: linkdb_data2 Text.
Mapper: <null, urls> --> <srcUrl, targetUrl>
Reducer: <srcUrl, Iterator<targetUrl>> --> <null, srcUrl+targetUrls>
*/
package OutLinksPackage;

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

public class OutLinks {
  public static class OutLinksMapper extends Mapper <LongWritable, Text, Text, Text> {
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String urls[] = line.split(" ");
      int urlSize = urls.length;
      for(int i = 1; i < urlSize; i++) {
  	context.write(new Text(urls[i]), new Text(urls[0]));
      }
    }
  }
  
  public static class OutLinksReducer extends Reducer <Text, Text, Text, NullWritable > {
    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuffer OutLinkStr = new StringBuffer(key.toString());
      for(Text value : values) {
        OutLinkStr.append(" ").append(value.toString());
      }
      context.write(new Text(OutLinkStr.toString()), NullWritable.get());
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
    
    job.setMapperClass(OutLinksMapper.class);
    job.setReducerClass(OutLinksReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.waitForCompletion(true);
  }
}
