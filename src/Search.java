/*
description: Search main code includes reading the pageRank and calculating the text matchValue.
input: pattern  _The search pattern
       PageRankMap  _<key,value> is <url, pageRank>
       parse_text _Text includes url and the parse text in the url page, separated by '\t'.
output: searchResult _Text includes the url and its weight.
Mapper: <null, url and parse_text> --> <weight, url>
Reducer: <weight, url> --> sorted <weight, url> 
*/
//package org.apache.hadoop.mapred;
package SearchPackage;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.nutch.parse.ParseText;

public class Search {
  
  private static String pattern = null;
  private static MapFileRead pageRankRead = null;


  public static class TextMatcher {
   /* private String pattern;
    private String text;
    public double matchVal;

    public TextMatcher(String pattern, String text) {
      this.pattern = pattern;
      this.text = text;
      this.matchVal = 0;
    }*/

    public static double calcMatchVal(String text, String Pattern) throws IOException, InterruptedException {
      int patternLen = pattern.length();
      int textLen = text.length();
      int tmpTextLen = 0;	//匹配的部分字符串在Text的总长度
      int tmpPatternLen = 0;		//除去前面完全匹配的匹配字符个数
      double matchVal = 0;

      for(int i = 0; i < textLen; i++) {
	tmpTextLen++;
	if(text.charAt(i) == pattern.charAt(tmpPatternLen)) {
	  tmpPatternLen++;
	  if(tmpPatternLen == 1) {
	    tmpTextLen = 1;
	  }
	  if(tmpPatternLen == patternLen || tmpTextLen == 3*patternLen) {
          //当匹配完成或者匹配了一定数量的字符仍然没有匹配成功则计算当前匹配值
	    matchVal += ((double)tmpPatternLen)/((double)tmpTextLen);
	    tmpPatternLen = 0;
	  }
	}
      }
      return matchVal;
    }
  }

  public static class SearchMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String text = value.toString();
      String url = new String();
      int i = 0;
      while(text.charAt(i) != ' ') {
        url = url+text.charAt(i);
        i++;
      }
      text = text.substring(++i,text.length());
      double urlMatchVal = TextMatcher.calcMatchVal(url, pattern);
      double textMatchVal = TextMatcher.calcMatchVal(text, pattern);
      double pageRank = pageRankRead.getValue(url);
      double urlWeight = Math.pow(urlMatchVal*10+textMatchVal, pageRank)+pageRank*1000;
      context.write(new DoubleWritable(urlWeight), new Text(url));
    }
  }

  public static class SearchReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
    job.setJarByClass(Search.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    pattern = args[2];

    job.setMapperClass(SearchMapper.class);
    job.setReducerClass(SearchReducer.class);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);
    
    pageRankRead = new MapFileRead("hdfs://localhost/input/PageRankMap");
    job.waitForCompletion(true);
  }
}
