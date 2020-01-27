package com.hadoop.learn.mr.distributedcache;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistributedCache_Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	// Developer [ value_list ]

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double totalIncrement = 0;
		int count = 0;

		Iterator<DoubleWritable> valuesIter = values.iterator();

		/* For each store location */
		while (valuesIter.hasNext()) {
			count++;
			totalIncrement += valuesIter.next().get();
		}

		double avgInc = totalIncrement / count;

		/* designation, average_increment */
		context.write(key, new DoubleWritable(avgInc));
	}
}
