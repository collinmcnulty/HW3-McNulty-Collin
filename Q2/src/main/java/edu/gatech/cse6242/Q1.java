package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Q1 {

  public static class Q1Mapper
  	extends Mapper<Object, Text, Text, IntWritable> {
	  
	  private final static IntWritable one = new IntWritable(1);
	  private Text tgt = new Text();
	  
	  public void map (Object key, Text value, Context context)
	  throws IOException, InterruptedException {
		  String line = value.toString();
		  String tokens[] = line.split("\t");
		  tgt.set(tokens[1]);
		  IntWritable weight = new IntWritable();
		  weight.set(Integer.parseInt(tokens[2]));
		  context.write(tgt, weight);
		  
	  }
  }
  
  public static class IntSumReducer 
  	extends Reducer<Text, IntWritable, Text, IntWritable> {
	  private IntWritable result = new IntWritable ();
	  
	  public void reduce(Text key, Iterable<IntWritable> values, 
			  Context context
			  ) throws IOException, InterruptedException {
		  int max = 0;
		  for (IntWritable val : values){
			  int value = val.get();
			  if (max < value){
				  max = value;
			  }
		  }
		  result.set(max);
		  context.write(key, result);
	  }
  }
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    //Stuff I wrote:
    job.setJarByClass(Q1.class);
    job.setMapperClass(Q1Mapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}