package SearchPackage;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TermsSelector {
   
    //public static final int termLength = 7;

    public static int charDecode(char c) {
    
      if((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
	//字符是单词的一部分
	   return 0;
	}
       else if (c >= '0' && c <= '9') {
           return 1;
       }
	else if(c >= 0x4E00 && c <= 0x9FBF) {
	//字符是中文字符
	   return 2;
        }
	return 3;
    }

    public static HashMap<String, Integer> termsSelect(StringBuilder parseText) {
        
      HashMap<String, Integer> terms = new HashMap<String, Integer>();
      StringBuilder term = new StringBuilder();
      Integer tmpTermLength = 0;
      
      int parseTextLength = parseText.length();
      if(parseTextLength == 0) {
        return terms;
      }
      parseText.append(" ");

      for (int termLength = 3;termLength <= 7; termLength += 4) {
      int i = 0;
      int nowParse,nextParse;
      nextParse = charDecode(parseText.charAt(0));

      while(i < parseTextLength) {

        nowParse = nextParse;
        nextParse = charDecode(parseText.charAt(i+1));

        if (nowParse != 3) {
     
          term.append(parseText.charAt(i));

          if (nowParse == 2 || nowParse != nextParse) {

            term.append(" ");
            tmpTermLength++;

	    if (tmpTermLength == termLength) {

	       Integer termNum = terms.get(new String(term));
	       if(termNum != null) {
		 terms.put(new String(term), ++termNum);
	       }
	       else {
		 terms.put(new String(term), new Integer(1));
                 if(terms.size() > 100) {
		    return terms;
 		 }
	       }    
	       term.setLength(0);
               tmpTermLength = 0;
	    }
          }
        }
	i++;
      }
      }
      return terms;
    }

  static class TermsSelectorMapper extends Mapper <LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringBuilder url = new StringBuilder();
      int i = 0;
      String parse_text = value.toString();

      while(parse_text.charAt(i) != ' ') {
        url.append(parse_text.charAt(i++));
      }
      url.append(" ");

      HashMap<String, Integer> terms = termsSelect(new StringBuilder(parse_text.substring(i+1, parse_text.length())));

      Iterator iter = terms.entrySet().iterator();

      while(iter.hasNext()) {
        Map.Entry entry = (Map.Entry) iter.next();
	String tmpTerm = (String) entry.getKey();
	String termNum = entry.getValue().toString();
	if (tmpTerm != null) {
          StringBuilder tmpUrl = new StringBuilder(url);
	  context.write(new Text(tmpTerm), new Text(new String(tmpUrl.append(termNum))));
	}  
      }
    }
  }

  static class TermsSelectorReducer extends Reducer<Text, Text, Text, NullWritable> {
    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuilder invertedIndex = new StringBuilder(key.toString());
      for(Text value : values) {
 	invertedIndex.append("\t").append(new StringBuilder(value.toString()));
      }
      context.write(new Text(new String(invertedIndex)), NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage : TermsSelector <input path> <output path>");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    Job job = new Job(conf, "TermsSelect");
    job.setJarByClass(TermsSelector.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
    job.setMapperClass(TermsSelectorMapper.class);
    job.setReducerClass(TermsSelectorReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);

    job.waitForCompletion(true); 
  }
}
