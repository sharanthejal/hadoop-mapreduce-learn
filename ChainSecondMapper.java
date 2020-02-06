package com.hadoop.learn.mr.chainmapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChainSecondMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

		String word = key.toString();
		String startingLetter = word.substring(0, 1);

		context.write(new Text(startingLetter), ONE);

	}

}
