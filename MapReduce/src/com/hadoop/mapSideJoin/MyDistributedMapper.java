package com.hadoop.mapSideJoin;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
@SuppressWarnings("deprecation")
public  class MyDistributedMapper extends Mapper<LongWritable,Text, Text, Text> {
	
	private Map<String, String> abMap = new HashMap<String, String>();
			private Text outputKey = new Text();
			private Text outputValue = new Text();
	
	protected void setup(Context context) throws java.io.IOException, InterruptedException{
	Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path p : files) {
			if (p.getName().equals("lookup.dat")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) {
					String[] tokens = line.split("\t");
					String ab = tokens[0];
					String state = tokens[1];
					abMap.put(ab, state);
					line = reader.readLine();
				}
			}
		}
		if (abMap.isEmpty()) {
			throw new IOException("Unable to Data.");
		}
	}
    protected void map(LongWritable key, Text value, Context context)
        throws java.io.IOException, InterruptedException {
    	String row = value.toString();
    	String[] tokens = row.split("\t");
    	String inab = tokens[0];
//		String population = tokens[1];
    	String state = abMap.get(inab);
    	outputKey.set(state);
    	outputValue.set(row);
//		outputValue.set(population);
  	  	context.write(outputKey,outputValue);
    }  
}