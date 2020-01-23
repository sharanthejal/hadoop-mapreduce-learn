package com.hadoop.learn.mr.customwritables;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WC_ReducerCustomWritable extends Reducer<CustomTextWritable, IntWritable, CustomTextWritable, IntWritable> {

	
	@Override
	protected void reduce(CustomTextWritable key, Iterable<IntWritable> values, Context context
            ) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable x: values)
		{
		sum+=x.get();
		}
		context.write(key, new IntWritable(sum));
		
	}
}
