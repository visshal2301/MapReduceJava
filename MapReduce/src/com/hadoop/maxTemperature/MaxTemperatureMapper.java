package com.hadoop.maxTemperature;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {
	private static final int MISSING=9999;
	
	public void map(LongWritable Key,Text value,OutputCollector <Text , IntWritable>output, Reporter reporter) throws IOException{
		String line=value.toString();
		String year=line.substring(7,10);
		int airTemperature;
		
			airTemperature=Integer.parseInt(line.substring(12,13));
		
			output.collect(new Text(year), new IntWritable(airTemperature));
			
		
	}

}
