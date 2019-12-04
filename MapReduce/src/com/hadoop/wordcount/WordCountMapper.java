package com.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	private Text word = new Text();
	public void map(LongWritable Key,Text value,Context context) throws IOException,InterruptedException{
		String line=value.toString();
		System.out.println("I am in Mapper"+line);
		StringTokenizer tokenizer=new StringTokenizer(line);
		System.out.println("I am in Tokenizer"+tokenizer);
		
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			context.write(word, new IntWritable(1));
		}				
		
	}
}
