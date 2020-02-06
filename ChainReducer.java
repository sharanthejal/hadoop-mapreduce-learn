package com.hadoop.learn.mr.chainmapper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChainReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	// {W, [1, 1, 1, 1......]}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		Iterator<IntWritable> itr= values.iterator();
		int count=0;
		
		while(itr.hasNext()) {
			count +=itr.next().get();
		}
		
		context.write(key, new IntWritable(count));
	}

}
