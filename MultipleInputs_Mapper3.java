package com.hadoop.learn.mr.multipleinputs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleInputs_Mapper3 extends Mapper<Text, Text, Text, IntWritable> {

	private IntWritable ONE=new IntWritable(1);
	
	/* Here reading is done using KeyValueTextInputFormatClass */
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		context.write(value, ONE);
	}

}
