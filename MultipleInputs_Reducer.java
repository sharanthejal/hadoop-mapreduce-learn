package com.hadoop.learn.mr.multipleinputs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * This is like a common reducer for word count program. No change
 */
public class MultipleInputs_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/*
	 * Input will be in the below format as there is no combiner Sharan,
	 * [1,1,1,1,1...]
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		int count = 0;
		Iterator<IntWritable> itr = values.iterator();

		while (itr.hasNext()) {
			count++;
		}

		context.write(key, new IntWritable(count));
	}

}
