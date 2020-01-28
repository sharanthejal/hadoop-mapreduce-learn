package com.hadoop.learn.mr.multipleoutputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MultipleOutputs_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String files[] = new GenericOptionsParser(conf, args).getRemainingArgs();

		Path inputPath = new Path(files[0]);
		Path outputPath = new Path(files[1]);

		Job job = Job.getInstance(conf, "Multiple_Outputs_Hadoop_MR");
		job.setJarByClass(MultipleOutputs_Driver.class);

		// Set Mapper and Reducer Class
		job.setMapperClass(MultipleOutputs_Mapper.class);
		job.setReducerClass(MultipleOutputs_Reducer.class);

		// Set output classes for mapper and reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set Input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set
		FileInputFormat.addInputPath(job, inputPath);

		MultipleOutputs.addNamedOutput(job, "HR", TextOutputFormat.class, Text.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "Accounts", TextOutputFormat.class, Text.class, Text.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		// Run the Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
