
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

public class TermsSearch {
	
    private static String pattern = null;
    private static MapFileRead urlModulus = null;
    private static MapFileRead pageRanks = null;
    private static final int termsNum = UrlModulus.termsNum;

    public static void inToPattern (StringBuilder input) {
      StringBuilder tmpPattern = new StringBuilder();
      int i = 0;
      int nowParse;
      int nextParse = TermsSelector.charDecode(input.charAt(i));
      int patternLength = input.length();
      input.append(" ");
      while(i < patternLength) {
	nowParse = nextParse;
        nextParse = TermsSelector.charDecode(input.charAt(i+1));
	if (nowParse != 3) {
	  tmpPattern.append(input.charAt(i));
	  if (nowParse == 2 || nowParse != nextParse) {
            tmpPattern.append(" ");
          }
	}
        i++;
      }
      pattern = new String(tmpPattern);
    }

    public static class TermsSearchMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
    
      public boolean IsMatch (String term) {
   	
	//String patternArray = pattern;
	String[] patternArray = pattern.split(" ");
	String[] termArray = term.split(" ");
    	double patternSize = patternArray.length;
    	double termSize = termArray.length;

	double sameSize = 0;

    	for (int i = 0;i < patternSize;i++) {
	  for (int j = 0;j < termSize;j++) {
	    if (patternArray[i].equals(termArray[j])) {
	      sameSize++;
	    }
	  }
        }
	if (sameSize > 0.45*Math.min(patternSize, termSize)) {
	   return true;
        }
	return false;
      }
    
      public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String[] urlFreqs = (value.toString()).split("\t");
    	double termFreq = urlFreqs.length-1;
    	double IDF = Math.log(termsNum/termFreq);
    	
    	if (IsMatch(urlFreqs[0])) {
    	  for (int i = 1;i <= termFreq;i++) {
    	     String[] urlFreq = urlFreqs[i].split(" ");
             double TF_IDF = (double)(Integer.parseInt(urlFreq[1]))*IDF;
    	     context.write(new Text(urlFreq[0]), new DoubleWritable(TF_IDF));
    	  }
        }
      }
   }
	
   public static class TermsSearchReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		
     public void reduce (Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	double urlPatternDotProduct = 0;
	for(DoubleWritable value : values) {
	  urlPatternDotProduct += value.get();
	}
	String url = key.toString();
	double urlPatternCosDistance = urlPatternDotProduct/(urlModulus.getValue(url));
	double pageRank = pageRanks.getValue(url);
			
	double urlWeight = Math.pow(urlPatternCosDistance, pageRank)+pageRank*10000;
			
	context.write(key, new DoubleWritable(urlWeight));
     }
   }
	
   public static void main(String[] args) throws Exception {
	if(args.length != 3) {
	  System.err.println("Usage: TermsSearch <input path> <output path> <input pattern>");
	  System.exit(-1);
	}
		
	Configuration conf = new Configuration();
	Job job = new Job(conf, "TermsSearch");
	job.setJarByClass(UrlModulus.class);
		
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	inToPattern(new StringBuilder(args[2]));

        urlModulus = new MapFileRead("/input/UrlVectorLength");
	pageRanks = new MapFileRead("/input/PageRankMap");
		
	job.setMapperClass(TermsSearchMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(DoubleWritable.class);
		
	job.setReducerClass(TermsSearchReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
		
	job.setNumReduceTasks(1);
	job.waitForCompletion(true);
   }
}
