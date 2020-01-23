package com.hadoop.learn.mr.fb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Facebook_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		String[] files=new GenericOptionsParser(conf, args).getRemainingArgs();
		Path inpath= new Path(files[0]);
		Path outpath= new Path(files[1]);
		
		Job job=Job.getInstance(conf, "Facebook_ads_success_avg");
		job.setJarByClass(Facebook_Driver.class);
		
		//Set Map output and Reducer output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Set Mapper and Reducer classes
		job.setMapperClass(Facebook_Mapper.class);
		job.setReducerClass(Facebook_Reducer.class);
		
		//set InputFormatClass
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set HDFS Inputpath and Output path from where data is Read Write to 
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		
		//Run the job 
		System.exit(job.waitForCompletion(true)?0:1);
		
		
		
	}

}
