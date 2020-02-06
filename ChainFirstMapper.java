package com.hadoop.learn.mr.chainmapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Word Count Program - word first letter count 
 */
public class ChainFirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] mapOutputKeys = value.toString().split(" ");

		for (String mapOutputKey : mapOutputKeys)
			context.write(new Text(mapOutputKey.toLowerCase().trim()), ONE);  //Word, 1

	}

}
