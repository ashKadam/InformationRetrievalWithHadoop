//
// Name: Ashwini Kadam
// Student ID: 800967986
// Email: akadam3@uncc.edu
//

package org.myorg;

import java.io.IOException;
import java.lang.Math;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.TreeSet;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Search extends Configured implements Tool {
        
	private static final String NUM_ARGS = "nArgs";	// number of arguments passed to find out user query
	private static final Logger LOG = Logger .getLogger( Search.class);

   	public static void main( String[] args) throws  Exception {
      		int res  = ToolRunner .run( new Search(), args);
      		System .exit(res);
   	}

   	public int run( String[] args) throws  Exception {
		
      		Job tfJob  = Job .getInstance(getConf(), " tf ");
	      	tfJob.setJarByClass( this .getClass());
	      	FileInputFormat.addInputPaths(tfJob,  args[0]);
	      	FileOutputFormat.setOutputPath(tfJob,  new Path("intermediate1")); // Store output of term frequency as intermediate input for tfidf
	      	tfJob.setMapperClass( tfMap .class);
	      	tfJob.setReducerClass( tfReduce .class);
	      	tfJob.setOutputKeyClass( Text .class);
	      	tfJob.setOutputValueClass( IntWritable .class);
	      	tfJob.waitForCompletion(true);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path pt = new Path(args[0]);
		ContentSummary cs = fs.getContentSummary(pt);
		int fileCount = (int)cs.getFileCount(); // Calculate number of input files from file system content summary
		conf.setInt("totalDocs", fileCount);

	      	Job tfidfob  = Job .getInstance(conf, " tfidf ");
	      	tfidfob.setJarByClass( this .getClass());
	      	FileInputFormat.addInputPaths(tfidfob,  "intermediate1");
	      	FileOutputFormat.setOutputPath(tfidfob,  new Path("searchintermediate1")); // Store output of tfidf as input for Search
	      	tfidfob.setMapperClass( tfidfMap .class);
	      	tfidfob.setReducerClass( tfidfReduce .class);
	      	tfidfob.setOutputKeyClass( Text .class);
	      	tfidfob.setOutputValueClass( Text .class);
	      	tfidfob.waitForCompletion( true);

		Job jobSearch = Job.getInstance(getConf(), " search "); 
		jobSearch.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(jobSearch, "searchintermediate1"); // reading from the intermediate file
		FileOutputFormat.setOutputPath(jobSearch, new Path(args[1])); 

		String[] nArgs = new String[args.length - 2]; // declaring a array of string with length of args -2 as the first is the path of input and output
		for(int i = 2; i < args.length; i++){
			nArgs[i-2] = args[i];
		}
		jobSearch.getConfiguration().setStrings(NUM_ARGS,nArgs); // assigning to the final variable
	
		jobSearch.setMapperClass(MapSearch.class); 
		jobSearch.setReducerClass(ReduceSearch.class); 
		jobSearch.setOutputKeyClass(Text.class); 
		jobSearch.setOutputValueClass(Text.class); 

		return jobSearch.waitForCompletion(true) ? 0 : 1; 
	}
   
   	public static class tfMap extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      		private final static IntWritable one  = new IntWritable( 1);
      		private Text word  = new Text();

      		private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

	      	public void map( LongWritable offset,  Text lineText,  Context context)
			throws  IOException,  InterruptedException {

	 		String line  = lineText.toString();
		 	Text currentWord  = new Text();

		 	for ( String word  : WORD_BOUNDARY .split(line)) {
		    		if (word.isEmpty()) {
		       			continue;
		    		}
		    	
				currentWord  = new Text(word);
		    		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		    		currentWord.set(currentWord.toString() + "#####" + fileName); // Write current word along with file name.

			    	context.write(currentWord,one);
	 		}
		}
   	}

   	public static class tfReduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
		@Override 
      		public void reduce( Text word,  Iterable<IntWritable > counts,Context context)
         		throws IOException,  InterruptedException {
         		
			int termFrequency  = 0;
	 		double logFrequency = 0;

		 	for ( IntWritable termF  : counts) {
		    		termFrequency  += termF.get();
		 	}
	
			if(termFrequency ==0)
				logFrequency = 0;
			else
				logFrequency = 1+Math.log10(termFrequency);

		 	context.write(word, new DoubleWritable(logFrequency));
		}
   	}

	public static class tfidfMap extends Mapper<LongWritable , Text,  Text ,  Text > {
		private final static IntWritable one  = new IntWritable( 1);
		private Text word  = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern .compile("\\n");
		private static final Pattern WORD_DELIMIT = Pattern .compile("#####");

		public void map(  LongWritable offset,  Text lineText, Context context)
			throws  IOException,  InterruptedException {

	 		String line  = lineText.toString();
			Text currentWord  = new Text();

			for ( String word  : WORD_BOUNDARY .split(line)) {
    				if (word.isEmpty()) {
       					continue;
    				}

	    			String[] tokens = word.split("#####");
	    			currentWord  = new Text(tokens[0]);
			    	currentWord.set(currentWord.toString());
			    	Text valueWord = new Text();
			    	String[] tokens1 = tokens[1].split("\t");
			    	valueWord.set(tokens1[0] + "=" + tokens1[1].toString());
			    	context.write(currentWord ,  valueWord);
			 }
		}
	}

   	public static class tfidfReduce extends Reducer<Text ,  Text ,  Text ,  Text > {
     		@Override 
      		public void reduce( Text word,  Iterable<Text > values,Context context)
         		throws IOException,  InterruptedException {
        		Text termFrequency  = new Text(); 
			Configuration conf = context.getConfiguration();
			int numTotalDoc = conf.getInt("totalDocs", 0);
	
			ArrayList<Text> filesCountWithWords = new ArrayList<>();
			for (Text filecountwithword : values) {
				filesCountWithWords.add(new Text(filecountwithword.toString()));
			}
         
			for (Text files : filesCountWithWords) {
				String[] tokens = files.toString().split("=");
    				String fileName = tokens[0];
				double idf = Math.log10(1+(numTotalDoc/filesCountWithWords.size()));
				double tdidf = Double.parseDouble(tokens[1]) * idf;
    				termFrequency.set(Double.toString(tdidf));
				Text w1 = new Text(word);
		    		w1.set(word.toString() + "#####" + fileName);
				context.write(w1, new Text(termFrequency));
        	 	}
      		}
   	}

	public static class MapSearch extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		// Initial setup to assign the variables in argument to a TreeSet.
		// TreeSet is used to prevent redundant entries of tokens from user query 
		TreeSet<String> keysToSearch;

		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			String[] nArgs = context.getConfiguration().getStrings(NUM_ARGS);
			keysToSearch = new TreeSet<>();
			if(nArgs != null){			
				for(int i = 0; i < nArgs.length; i++){
					keysToSearch.add(nArgs[i]);
				}
			}
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentWord = new Text();
			Text currValueWord = new Text();
			if (line.isEmpty()) {
				return;
			}
			String[] splittingdata = line.toString().split("#####");
			if (splittingdata.length < 0) {
				return;
			}

			// Check if user specified token is present in any of the input data.
			// If found, write it to context with corresponding TFIDF value.
			if(keysToSearch.contains(splittingdata[0])){
				String[] splitagain = splittingdata[1].split("\t");
				currentWord = new Text(splitagain[0]);
				currValueWord = new Text(splitagain[1]);
				context.write(currentWord, currValueWord);
			}
	
		}
	}

	public static class ReduceSearch extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context) 
			throws IOException, InterruptedException {
			
			float sum = 0;		    	
			for (Text count : counts) {
		        	sum += Float.parseFloat(count.toString());
		  	}

			// Sum up the TFIDF values for given user query. 
		    	Text temp = new Text(sum + "");
		    	context.write(word, temp);
		}
	}
}

