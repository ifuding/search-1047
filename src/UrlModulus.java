
package SearchPackage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

public class UrlModulus {
	
  public  static final int termsNum = 1059279;
  
  public static class UrlModulusMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
      
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String[] urlFreqs = (value.toString()).split("\t");
    	double termFreq = urlFreqs.length-1;
    	double IDF = Math.log(termsNum/termFreq);
    	
    	for (int i = 1;i <= termFreq;i++) {
    	  String[] urlFreq = urlFreqs[i].split(" ");
    	  double TF_IDF = (double)(Integer.parseInt(urlFreq[1]))*IDF;
    	  context.write(new Text(urlFreq[0]), new DoubleWritable(TF_IDF));
    	}
    }
  }
	
  public static class UrlModulusReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		
    public void reduce (Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double urlVectorLength = 0;
      for(DoubleWritable value : values) {
	urlVectorLength += Math.pow(value.get(), 2);
      }
      context.write(key, new DoubleWritable(Math.sqrt(urlVectorLength)));
   }
  }
	
  public static void main(String[] args) throws Exception {
    if(args.length != 2) {
      System.err.println("Usage: UrlModulus <input path> <output path>");
      System.exit(-1);
    }
		
    Configuration conf = new Configuration();
    Job job = new Job(conf, "UrlModulus");
    job.setJarByClass(UrlModulus.class);
		
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputFormatClass(MapFileOutputFormat.class);
		
    job.setMapperClass(UrlModulusMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
		
    job.setReducerClass(UrlModulusReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
		
    job.setNumReduceTasks(1);
    job.waitForCompletion(true);
  }
}
