package com.hadoop.partitioner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PartitionerDriver {
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(PartitionerDriver.class);
		conf.setJobName("wordcount");

		// Forcing program to run 3 reducers
		conf.setNumReduceTasks(3);

		conf.setMapperClass(PartitionerMapper.class);
		conf.setCombinerClass(PartitionerReducer.class);
		conf.setReducerClass(PartitionerReducer.class);
		conf.setPartitionerClass(MyPartitioner.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 
		JobClient.runJob(conf);
	}

}
