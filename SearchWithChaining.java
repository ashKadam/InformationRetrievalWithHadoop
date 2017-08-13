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


public class SearchWithChaining extends Configured implements Tool {

	private static final String OUTPUT_PATH = "intermediate_output";
	private static final String NUM_ARGS = "nArgs";
	private static final Logger LOG = Logger .getLogger( SearchWithChaining.class);

   	public static void main( String[] args) throws  Exception {
		ToolRunner.run(new TermFrequency(), args);      // Chaining is done by calling term frequency and tfidf code.           
		ToolRunner.run(new TFIDF(), args);		// This output will be used as input for searching. 
      		int res  = ToolRunner .run( new SearchWithChaining(), args);
      		System .exit(res);
   	}

   	public int run( String[] args) throws  Exception {
		
      		Job jobSearch = Job.getInstance(getConf(), " searchWithChain "); 
		jobSearch.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(jobSearch, args[2]); // reading from the intermediate file
		FileOutputFormat.setOutputPath(jobSearch, new Path(args[3])); 

                //declaring a array of string with length of args -4 as the first 4 parameters are input, intermediate and output paths
		String[] nArgs = new String[args.length - 4]; 
		for(int i = 4; i < args.length; i++){
			nArgs[i-4] = args[i];
		}
		jobSearch.getConfiguration().setStrings(NUM_ARGS,nArgs); // assigning to the final variable
	
		jobSearch.setMapperClass(MapSearch.class); 
		jobSearch.setReducerClass(ReduceSearch.class); 
		jobSearch.setOutputKeyClass(Text.class); 
		jobSearch.setOutputValueClass(Text.class); 

		return jobSearch.waitForCompletion(true) ? 0 : 1; 
	}

	public static class MapSearch extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		// Initial setup to assign the variables in argument to a TreeSet 
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

