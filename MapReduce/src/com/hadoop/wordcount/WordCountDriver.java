package com.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCountDriver {
	
	public static void main (String args[]) throws Exception
	{
		Configuration conf=new Configuration();
		System.out.println("First");
		String [] otherArgs =new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job =new Job(conf,"word counter");
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		System.out.println("Vishal 1");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new  Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new  Path(otherArgs[1]));
		System.out.println("First");

		System.exit(job.waitForCompletion(true)? 0:1);
		
	}

}
