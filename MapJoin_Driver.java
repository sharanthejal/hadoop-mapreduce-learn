package com.hadoop.learn.mr.joins.map;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapJoin_Driver {

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		// add files to cache 
		//DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/sharan/store.txt"), conf);
		String files[] = new GenericOptionsParser(conf, args).getRemainingArgs();

		Path inputPath = new Path(files[0]);
		Path outPutpath = new Path(files[1]);

		// Create Job Instance with job name
		Job job = Job.getInstance(conf, "MapJoin_DistributedCache_Hadoop_MR");
		job.setJarByClass(MapJoin_Driver.class);
		
		//job.addCacheFile(new URI("hdfs:///localhost:8020/tmp/sharan/designation.txt"));
		job.addCacheFile(new URI("hdfs://quickstart.cloudera:8020/tmp/sharan/store.txt"));
		job.addCacheFile(new URI("hdfs://quickstart.cloudera:8020/tmp/sharan/product.txt"));
		
		
		//Set Mapper and Reducer classes
		job.setMapperClass(MapJoin_Mapper.class);
		job.setReducerClass(MapJoin_Reducer.class);
		
		// Set Mapoutput key and value classes and Reducers also
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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
