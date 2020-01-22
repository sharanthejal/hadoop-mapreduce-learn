package com.hadoop.learn.mr.evenodd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EvenOddCount_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/*
	 * Input to reduce function comes from partitioner in the below format EVEN,
	 * [34,56,78,344,566,8888......]
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		String even = "EVEN";
		int sum = 0;
		if (key.toString().equalsIgnoreCase(even)) {

			for (IntWritable value : values) {
				sum += value.get();
			}
		} else {
			for (IntWritable value : values) {
				sum += value.get();
			}
		}
		// Write sum of Even numbers to HDFS from Reducer
		context.write(key, new IntWritable(sum));
	}

}
