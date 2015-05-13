/*
description: if srcUrl[1],srcUrl[2],...,srcUrl[n] direct to targetUrl,
	     then pageRank[targetUrl] = 1-dampFactor+ \ 
	     dampFactor*sum{pageRank[srcUrl[i]]/outLinkNum[srcUrl[i]], 1 <= i <= n}.
input: OutLinksMap MapFile, <key, value> is <url, outLinkNum>
       PageRankMap MapFile, <key, value> is <url, pageRank>
       OutLinks Text, every single line contain the url and the urls \
       which the first url direct to.
outPut: PageRankMap.
Mapper: <null, urls> --> <url, pageRankPart>
Reducer: <url, Iterator<pageRankPart>> --> <url, pageRank> 
*/
package SearchPackage;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;


public class PageRank {

  private static MapFileRead PageRankRead = null;

  public static class PageRankMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String urls[] = line.split(" ");
      int urlSize = urls.length;
      double srcPageRank = PageRankRead.getValue(urls[0]);
      //urls[0] is the srcUrl.
      double outLinkNum = urlSize-1;
      double pageRank_part = srcPageRank/outLinkNum;
      for(int i = 1; i < urlSize; i++) {
        context.write(new Text(urls[i]), new DoubleWritable(pageRank_part));
      }
    }
  }
  
  public static class PageRankReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce (Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double pageRank = 0;
      double dampFactor = 0.85;
      for(DoubleWritable value : values) {
        pageRank += value.get();
      }
      pageRank = 1-dampFactor+dampFactor*pageRank;
      context.write(key, new DoubleWritable(pageRank));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: OutLinks <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(PageRank.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setOutputFormatClass(MapFileOutputFormat.class);
    job.setMapperClass(PageRankMapper.class);
    job.setReducerClass(PageRankReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    PageRankRead = new MapFileRead("/input/PageRankMap");
    //OutLinksRead = new MapFileRead("/input/OutLinksMap");
    
    job.waitForCompletion(true);
  }
}
