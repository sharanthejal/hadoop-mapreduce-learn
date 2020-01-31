package com.hadoop.learn.mr.joins.map;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MapJoin_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	// STR_1 Bangalore [ {280} {560} {456}.......]
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int totalRevenue=0;
		
		Iterator<IntWritable> itr=values.iterator();
		
		while(itr.hasNext()) {
			
			totalRevenue+= itr.next().get();
			
		}
		
		context.write(key, new IntWritable(totalRevenue));
	}
}
