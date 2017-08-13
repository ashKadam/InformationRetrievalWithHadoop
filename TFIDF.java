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
import java.io.File;

import org.apache.hadoop.fs.ContentSummary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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

// This class calculates and display term frequncy of all words present in given input file(s).
public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
      ToolRunner.run(new TermFrequency(), args); // Chaining between map reduce is achieved by calling term frequency function.
      int res  = ToolRunner.run( new TFIDF(), args);
      System.exit(res);
   }

   public int run( String[] args) throws  Exception {

        // Calculated number of input files given by user and store it in cofig variable. 
	//It is required in the calcualtion TFIDF.
      	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	Path pt = new Path(args[0]);
	ContentSummary cs = fs.getContentSummary(pt);
	int fileCount = (int)cs.getFileCount();
      	conf.setInt("numDocs", fileCount);

	Job tfidfJob  = Job .getInstance(conf, " tfidf ");
      	tfidfJob.setJarByClass( this .getClass());
      	FileInputFormat.addInputPaths(tfidfJob,  args[1]); // Output of Term frequency is treated as input for TFIDF
      	FileOutputFormat.setOutputPath(tfidfJob,  new Path(args[2]));
      	tfidfJob.setMapperClass( TFIDFMap.class);
      	tfidfJob.setReducerClass( TFIDFReduce.class);
      	tfidfJob.setOutputKeyClass( Text.class);
      	tfidfJob.setOutputValueClass( Text.class);

      	return tfidfJob.waitForCompletion( true)  ? 0 : 1;
   }

      public static class TFIDFMap extends Mapper<LongWritable , Text,  Text ,  Text > {
      	private final static IntWritable one  = new IntWritable( 1);
      	private Text word  = new Text();

      	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\n");

      	public void map(  LongWritable offset,  Text lineText, Context context)
        	throws  IOException,  InterruptedException {
            
         	String line  = lineText.toString();
	        Text currentWord  = new Text();
		for ( String word  : WORD_BOUNDARY.split(line)) {
            		if (word.isEmpty()) {
               		    continue;
            		}

		// Split current line into word and fileName+termFrequency
            	String[] tokens = line.split("#####");
            	currentWord  = new Text(tokens[0]);
	    	currentWord.set(currentWord.toString());

		// Split fileName+termFrequency by using '\t' as splitting token.
		// First token of this split would be file name and other would be term frequency
    		Text valueWord = new Text();
	    	String[] tokens1 = tokens[1].split("\t");
	    	valueWord.set(tokens1[0] + "=" + tokens1[1].toString());
		context.write(currentWord ,  valueWord);

	}// end of for loop
      }// end of map function
   }// end of class TFIDFMap

   public static class TFIDFReduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text> values, Context context)
         throws IOException,  InterruptedException {
         Text termFrequency  = new Text(); 
	Configuration conf=context.getConfiguration();
	int numTotalDocs = conf.getInt("numDocs", 0);
	//int  = Integer.parseInt(totalDoc);
	
	// Iterate over all and store them in array list. 
	// By doing so, we can get number of files in which word(key) is present. It is required to calculate tfidf.
	ArrayList<Text> fileNameWithTermFreq = new ArrayList<>();
	for (Text fileAndTermFreq : values) {
		fileNameWithTermFreq.add(new Text(fileAndTermFreq.toString()));
	}

         
	for (Text files : fileNameWithTermFreq) {
		String[] tokens = files.toString().split("=");
    		String fileName = tokens[0];
                
                // Calculate tfidf  using formula tfidf = termFrequency * idf. Num of input docs is caluculated earlier. 
		double idf = Math.log10(1+(numTotalDocs/fileNameWithTermFreq.size()));
		double tdidf = Double.parseDouble(tokens[1]) * idf;
    		termFrequency.set(Double.toString(tdidf));

		Text w1 = new Text(word);
		String v1 = word + "#####" + fileName;
    		w1.set(new Text(v1));
		context.write(w1, new Text(termFrequency));

         }// end of for loop
      }// end of reduce function
   }// end of class TFIDFReduce
}// end of class TFIDF

