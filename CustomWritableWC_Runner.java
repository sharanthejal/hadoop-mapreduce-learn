package com.hadoop.learn.mr.customwritables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Careful about imports for mapreduce.lib.input.FileInputFormat or TextOutputFormat 
 * to get it from mapreduce or mapred
 */
public class WC_RunnerCustomWritable {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: WC_Runner <input path> <output path>");
			System.exit(-1);
			}
		
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf,"customWritableWordCount");
		
		job.setJarByClass(WC_RunnerCustomWritable.class);
		
		job.setMapperClass(WC_MapperCustomWritable.class);
		job.setCombinerClass(WC_ReducerCustomWritable.class); //This is optional
		job.setReducerClass(WC_ReducerCustomWritable.class);
		
		job.setMapOutputKeyClass(CustomTextWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(CustomTextWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outputPath = new Path(args[1]);
		// Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// deleting the output path automatically from hdfs so that we don't have to
		// delete it explicitly
		//outputPath.getFileSystem(conf).delete(outputPath);
		// exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
