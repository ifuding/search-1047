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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

  public static class MapFileRead {
    
    private MapFile.Reader reader = null;
    private DoubleWritable value = null;

    private Configuration conf = null;
    private FileSystem fs = null;
    private Path path = null;
    
    public MapFileRead(String uri) throws IOException{
      this.conf = new Configuration();
      this.fs = FileSystem.get(URI.create(uri), conf);
      this.path = new Path(uri);
      this.reader = new MapFile.Reader(fs, uri, conf);
    }
    public double getValue(String url) throws IOException{
      try {
        reader.get(new Text(url), value);
      }   
      finally {
        if(value == null) {
          return 1;
        }
        return value.get();
      }
    }
  }

  public static class TextMatcher {
    private String pattern;
    private String text;
    public double matchVal;

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
	    matchVal += ((double)tmpPatternLen)/((double)tmpTextLen);
	    tmpPatternLen = 0;
	  }
	}
      }
    }
  }

  public static class SearchMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String text = value.toString();
      String url = new String();
      int i = 0;
      while(text.charAt(i) != '\t') {
        url = url+text.charAt(i);
        i++;
      }
      text = text.substring(++i,text.length());
      TextMatcher matcher = new TextMatcher(pattern,text);
      matcher.calcMatchVal();
      double pageRank = pageRankRead.getValue(url);
      context.write(new DoubleWritable(pageRank*(matcher.matchVal)), new Text(url));
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
    
    pageRankRead = new MapFileRead("input/PageRankMap");
    job.waitForCompletion(true);
  }
}
