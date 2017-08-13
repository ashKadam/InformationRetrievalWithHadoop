//
// Name: Ashwini Kadam
// Student ID: 800967986
// Email: akadam3@uncc.edu
//
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.MapContext;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.TreeSet;


public class Rank extends Configured implements Tool {

	private static final String OUTPUT_PATH = "intermediate_output";
	private static final String NUM_ARGS = "nArgs"; // number of arguments passed to find out user query
	private static final Logger LOG = Logger .getLogger( Rank.class);

   	public static void main( String[] args) throws  Exception {
      		int res  = ToolRunner .run( new Rank(), args);
      		System .exit(res);
   	}

   	public int run( String[] args) throws  Exception {
		
      		Job job1  = Job .getInstance(getConf(), " tf ");
	      	job1.setJarByClass( this .getClass());
	      	FileInputFormat.addInputPaths(job1,  args[0]);
	      	FileOutputFormat.setOutputPath(job1,  new Path("intermediate1")); // Store output of term frequency as intermediate input for tfidf
	      	job1.setMapperClass( Map1 .class);
	      	job1.setReducerClass( Reduce1 .class);
	      	job1.setOutputKeyClass( Text .class);
	      	job1.setOutputValueClass( IntWritable .class);
	      	job1.waitForCompletion(true);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path pt = new Path(args[0]);
		ContentSummary cs = fs.getContentSummary(pt);
		int fileCount = (int)cs.getFileCount(); // Calculate number of input files from file system content summary
		conf.setInt("totalDocs", fileCount);

		Job job2  = Job .getInstance(conf, " tfidf ");
	      	job2.setJarByClass( this .getClass());
	      	FileInputFormat.addInputPaths(job2,  "intermediate1");
	      	FileOutputFormat.setOutputPath(job2,  new Path("searchintermediate1"));
	      	job2.setMapperClass( Map2 .class);
	      	job2.setReducerClass( Reduce2 .class);
	      	job2.setOutputKeyClass( Text .class);
	      	job2.setOutputValueClass( Text .class);
	      	job2.waitForCompletion( true);

		Job jobSearch = Job.getInstance(getConf(), " search "); 
		jobSearch.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(jobSearch, "searchintermediate1"); // reading from the intermediate file
		FileOutputFormat.setOutputPath(jobSearch, new Path("rankintermediate1")); 

		String[] nArgs = new String[args.length - 2]; // declaring a array of string with length of args -2 as the first is the path of input and output
		for(int i = 2; i < args.length; i++){
			nArgs[i-2] = args[i];
		}
		jobSearch.getConfiguration().setStrings(NUM_ARGS,nArgs); // assigning to the final variable
	
		jobSearch.setMapperClass(MapSearch.class); 
		jobSearch.setReducerClass(ReduceSearch.class); 
		jobSearch.setOutputKeyClass(Text.class); 
		jobSearch.setOutputValueClass(Text.class);
		jobSearch.waitForCompletion(true); 

		Job jobRank  = Job .getInstance(conf, " rank ");
	      	jobRank.setJarByClass( this .getClass());
	      	FileInputFormat.addInputPaths(jobRank,  "rankintermediate1");
	      	FileOutputFormat.setOutputPath(jobRank,  new Path(args[1]));
	      	jobRank.setMapperClass( MapRank .class);
	      	jobRank.setReducerClass( ReduceRank .class);
		jobRank.setMapOutputKeyClass(FloatWritable.class);
		jobRank.setMapOutputValueClass(Text.class);
	      	jobRank.setOutputKeyClass( Text .class);
	      	jobRank.setOutputValueClass( FloatWritable .class);


		return jobRank.waitForCompletion(true) ? 0 : 1;
	}
   
   	public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
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
		    		currentWord.set(currentWord.toString() + "#####" + fileName);

			    	context.write(currentWord,one);
	 		}
		}
   	}

   	public static class Reduce1 extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
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

	public static class Map2 extends Mapper<LongWritable , Text,  Text ,  Text > {
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

   	public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text > {
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
				context.write(w1, new Text(termFrequency));
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
		    	Text temp = new Text(sum + "");
		    	context.write(word, temp);
		}
	}

	public static class MapRank extends Mapper<LongWritable, Text, FloatWritable, Text> {
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentWord = new Text();
			if (line.isEmpty()) {
				return;
			}
			String[] splittingdata = line.toString().split("\t");
			if (splittingdata.length < 0) {
				return;
			}
			currentWord.set(splittingdata[0]);

			// In order to rank the results in decreasing order, we need to write value first. 
			// As we are multipying it with -1, while writing, context will sort it based on value .
			float value = Float.parseFloat(splittingdata[1]) * (-1); 
			context.write(new FloatWritable(value),currentWord);
	
		}
	}

	public static class ReduceRank extends Reducer<FloatWritable, Text, Text, FloatWritable> {

		public void reduce(FloatWritable value, Iterable<Text> counts, Context context) 
			throws IOException, InterruptedException {
			float temp = 0;	
			Text word1 = new Text();
	
			// Get back original value after sorting is done in map function.	
			temp = value.get() * -1;
			for (Text word : counts){
				word1= new Text(word);
				context.write(word1, new FloatWritable(temp));	
			}	
		}
	}
}

