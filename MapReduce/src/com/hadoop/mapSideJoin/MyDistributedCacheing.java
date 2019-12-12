package com.hadoop.mapSideJoin;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDistributedCacheing {


  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
    Job job = new Job();
    job.setJarByClass(MyDistributedCacheing.class);
    job.setJobName("DCTest");
    job.setNumReduceTasks(0);
    
    try{
    DistributedCache.addCacheFile(new URI("/input/lookup.dat"), job.getConfiguration());
    }catch(Exception e){
    	System.out.println(e);
    }
    
    job.setMapperClass(MyDistributedMapper.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    
  }
}

