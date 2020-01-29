package com.hadoop.learn.mr.joins.outer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class OuterJoin_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf= new Configuration();
		
		String files[]= new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Path inputPath1=new Path(files[0]);
		Path inputPath3=new Path(files[1]);
		Path outputPath=new Path(files[2]);
		
		Job job=Job.getInstance(conf, "Outer_Join_Hadoop_MR");
		job.setJarByClass(OuterJoin_Driver.class);
		
		//Set Mapper and Reducer class
		job.setMapperClass(OuterJoinEmp_Mapper.class);
		job.setMapperClass(OuterJoinDept_Mapper.class);
		job.setReducerClass(OuterJoin_Reducer.class);
		
		//set mapper and reducer output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Set Inpout and Output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Multiple fileinputs
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, OuterJoinEmp_Mapper.class);
		
		MultipleInputs.addInputPath(job, inputPath3, TextInputFormat.class, OuterJoinDept_Mapper.class);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Run job and wait for completion
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
