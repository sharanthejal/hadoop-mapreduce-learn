package com.hadoop.learn.mr.frauddetection;

import java.io.IOException;

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

public class FraudDetection_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf= new  Configuration();
		String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		//Create Job instance
		Job job=Job.getInstance(conf);
		job.setJarByClass(FraudDetection_Driver.class);
		job.setJobName("FraudDetection_orders_score");
		
		//Set MapOutputKeyClass and Value class and also reducers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FraudDetection_Writable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//Set Mapper and Reducer classes
		job.setMapperClass(FraudDetection_Mapper.class);
		job.setReducerClass(FraudDetection_Reducer.class);
		
		//set InputFormat class and OutputFormatClass
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set InputPath and OutputPath for getting and putting data from and to HDFS
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		//start Job and exit 
		//Submit the job to the cluster and wait for it to finish.
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
