package com.hadoop.learn.mr.chainmapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ChainDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path inputPath = new Path(files[0]);
		Path outputPath = new Path(files[0]);

		Job job = Job.getInstance(conf, "Chain MR Program");
		job.setJarByClass(ChainDriver.class);

		// Set input and outputKeyFormat for Reducer
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set Mappers and Reducers
		ChainMapper.addMapper(job, ChainFirstMapper.class, LongWritable.class, Text.class, Text.class,
				IntWritable.class, conf);

		ChainMapper.addMapper(job, ChainSecondMapper.class, Text.class, IntWritable.class, Text.class,
				IntWritable.class, conf);

		ChainReducer.setReducer(job, ChainReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class,
				conf);

		// Set inputpath and output dir from where data should be read and write into
		// HDFS
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// Run Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		

	}

}
