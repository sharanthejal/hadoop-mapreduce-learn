package com.hadoop.learn.mr.multipleinputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class MultipleInputs_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf=new Configuration();

		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		String[] files= new GenericOptionsParser(conf, args).getRemainingArgs();
		
		//Set Input and output path
		Path inputPath1=new Path(files[0]);
		Path inputPath3=new Path(files[1]);
		Path outputPath= new Path(files[2]);
		
		Job job=Job.getInstance(conf, "MultipleInputs_MR_JOB");
		job.setJarByClass(MultipleInputs_Driver.class);
		
		//Set Mapper Class and Reducer Class
		job.setMapperClass(MultipleInputs_Mapper1.class);
		job.setMapperClass(MultipleInputs_Mapper3.class);
		job.setReducerClass(MultipleInputs_Reducer.class);
		
		//set map and reducer output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Set Input and Output Format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Add input and outpaths from where hadoop job reads and writes the data from and to HDFS		
		
		//FileInputFormat can't be used, MultipleInputs need to be used for this
		
		/*FileInputFormat.addInputPath(job, inputPath1);
		FileInputFormat.addInputPath(job, inputPath3);*/

		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MultipleInputs_Mapper1.class);
		
		MultipleInputs.addInputPath(job, inputPath3, KeyValueTextInputFormat.class, MultipleInputs_Mapper3.class);
		

		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		
		//Run Job
		System.exit(job.waitForCompletion(true)?0:1);
		
		
		
	}

}
