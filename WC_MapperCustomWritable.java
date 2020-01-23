package com.hadoop.learn.mr.customwritables;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WC_MapperCustomWritable extends Mapper<LongWritable, Text, CustomTextWritable, IntWritable> {
	private final static IntWritable ONE = new IntWritable(1);
	//private CustomTextWritable word = new CustomTextWritable();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			//For the custom class below code is changed
			//word.setWord(tokenizer.nextToken());

			context.write(new CustomTextWritable(tokenizer.nextToken()), ONE);
		}
	}

}