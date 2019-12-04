package com.hadoop.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MyPartitioner implements Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		String myKey = key.toString().toLowerCase();

		if ((myKey.equalsIgnoreCase("delhi")) || (myKey.equalsIgnoreCase("mumbai"))) {
			return 0; // hadoop,1 and learning,1
		}
		if ((myKey.equalsIgnoreCase("kolkata")) || (myKey.equalsIgnoreCase("chennai"))) {
			return 1; // data,1 and science,1
		} else {
			return 2;// We,1 are,1 only,1 and,1 not,1 focusing,1 on,1
		}
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}
}