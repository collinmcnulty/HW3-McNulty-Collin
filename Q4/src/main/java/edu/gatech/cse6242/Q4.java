package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {

  public static class Mapper1
  	extends Mapper<Object, Text, Text, IntWritable> {
	  
	  private final static IntWritable one = new IntWritable(1);
	  private Text tgt = new Text();
	  
	  public void map (Object key, Text value, Context context)
	  throws IOException, InterruptedException {
		  String line = value.toString();
		  String tokens[] = line.split("\t");
		  Text weight = new Text();
		  Text wordup = new Text();
		  weight.set(tokens[0]);
		  if (tokens.length>1){
			wordup.set(tokens[1]);
			context.write(wordup, one);
		  }
		  context.write(weight, one);
		  
	  }
  }
  
  public static class IntSumReducer
       extends Reducer<Text,IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  public static class Mapper2
  	extends Mapper<Object, Text, Text, IntWritable> {
	  
	  private final static IntWritable one = new IntWritable(1);
	  private Text tgt = new Text();
	  
	  public void map (Object key, Text value, Context context)
	  throws IOException, InterruptedException {
		  String line = value.toString();
		  String tokens[] = line.split("\t");
		  Text weight = new Text();
		  weight.set(tokens[1]);
		  context.write(weight, one);
		  
	  }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Q4");
	
	//Stuff I wrote for Q1 and modified:
    job1.setJarByClass(Q4.class);
    job1.setMapperClass(Mapper1.class);
    job1.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(IntSumReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("intermediate9"));
    job1.waitForCompletion(true);
	//
    

	
	

    /* 2nd Job */
	Job job2 = Job.getInstance(conf, "Q4");
	job2.setJarByClass(Q4.class);
    job2.setMapperClass(Mapper2.class);
    job2.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    //

    FileInputFormat.addInputPath(job2, new Path("intermediate9" + "/part-r-00000"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
