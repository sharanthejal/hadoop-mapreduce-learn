package com.hadoop.learn.mr.distributedcache;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DistributedCache_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Configuration conf = new Configuration();
		// add files to cache 
		//DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/jivesh/designation.txt"), conf);
		String files[] = new GenericOptionsParser(conf, args).getRemainingArgs();

		Path inputPath = new Path(files[0]);
		Path outPutpath = new Path(files[1]);

		// Create Job Instance with job name
		Job job = Job.getInstance(conf, "DistributedCacheForSmallFiles");
		job.setJarByClass(DistributedCache_Driver.class);
		
		//job.addCacheFile(new URI("hdfs:///localhost:8020/tmp/sharan/designation.txt"));
		job.addCacheFile(new URI("hdfs://quickstart.cloudera:8020/tmp/sharan/designation.txt"));
		
		
		//Set Mapper and Reducer classes
		job.setMapperClass(DistributedCache_MapperNew.class);
		job.setReducerClass(DistributedCache_Reducer.class);
		
		// Set Mapoutput key and value classes and Reducers also
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Set InputFormat and OutFormat classes.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Add inputPath and outputpath from where data is read and write to HDFS
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPutpath);

		// Execute Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
