//
// Name: Ashwini Kadam
// Student ID: 800967986
// Email: akadam3@uncc.edu
//

package org.myorg;

import java.io.IOException;
import java.lang.Math;
import java.util.regex.Pattern;

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

// This class calculates and display term frequncy of all words present in given input file(s).
public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " termfrequency "); // Job to calculate term frequency on all word
      job.setJarByClass( this .getClass());

      /// Set input and output files given by user
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      /// Map function maps the word(key) with fileName and term frequency. 
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            currentWord  = new Text(word);
	    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); // get file name in which word is present
	    currentWord.set(currentWord.toString() + "#####" + fileName); // concate the file name with word 

            context.write(currentWord,one); // While mapping, each word is treated as found to be once  
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,Context context)
         throws IOException,  InterruptedException {
         double termFrequency  = 0;
	 double totalCount = 0;
	
	 // Sum up all the counts
         for ( IntWritable count  : counts) {
            totalCount  += count.get();
         }
	
	// Calculate the term frequency using counts. 
	if(totalCount == 0)
		termFrequency = 0;
	else
		termFrequency = 1+Math.log10(totalCount);

         context.write(word, new DoubleWritable(termFrequency));
      }
   }
}

