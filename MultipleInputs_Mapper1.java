package com.hadoop.learn.mr.multipleinputs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MultipleInputs_Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private IntWritable ONE = new IntWritable(1);

	/*
	 * input to map function will be line by line. First line will be read and written 
	 * to intermediate disk using context.write -- Word Count program
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String[] words = value.toString().split(" ");

		for (String word : words)
			context.write(new Text(word), ONE);
	}

}
