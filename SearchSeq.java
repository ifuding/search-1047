//package org.apache.hadoop.mapred;
package SearchSeqPackage;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.nutch.parse.ParseText;

public class SearchSeq {
  
  private static String pattern = "baidu";

  public static class TextMatcher {
    private String pattern;
    private String text;
    public float matchVal;

    public TextMatcher(String pattern, String text) {
      this.pattern = pattern;
      this.text = text;
      this.matchVal = 0;
    }	
    public void calcMatchVal() throws IOException, InterruptedException {
      int patternLen = pattern.length();
      int textLen = text.length();
      int tmpTextLen = 0;	//匹配字符串在Text的总长度
      int tmpPatternLen = 0;		//除去前面完全匹配的匹配字符个数

      for(int i = 0; i < textLen; i++) {
	tmpTextLen++;
	if(text.charAt(i) == pattern.charAt(tmpPatternLen)) {
	  tmpPatternLen++;
	  if(tmpPatternLen == 1) {
	    tmpTextLen = 1;
	  }
	  if(tmpPatternLen == patternLen) {
	    matchVal += ((float)tmpPatternLen)/((float)tmpTextLen);
	    tmpPatternLen = 0;
	  }
	}
      }
    }
  }

  public static class SearchMapper extends Mapper<Text, ParseText, FloatWritable, Text> {
    
    public void map(Text key, ParseText value, Context context) throws IOException, InterruptedException {
      String text = value.toString();
      TextMatcher matcher = new TextMatcher(pattern,text);
      matcher.calcMatchVal();
      context.write(new FloatWritable(matcher.matchVal), key);
    }
  }

  public static class SearchReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {
    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for(Text value : values) {
        context.write(key, value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if(args.length != 3) {
      System.err.println("Usage: Search <input> <output> <pattern>");
      System.exit(-1);
    }
    
 
    Configuration conf = new Configuration();
    Job job = new Job(conf,"Search");
    job.setJarByClass(SearchSeq.class);

   // FileInputFormat.addInputPath(job, new Path(args[0]));
   // SequenceFileInputFormat.listStatus(job);
    SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    pattern = args[2];

    job.setMapperClass(SearchMapper.class);
    job.setReducerClass(SearchReducer.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);
    
    job.waitForCompletion(true);
  }
}
