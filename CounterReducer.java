package com.hadoop.learn.mr.counters;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CounterReducer extends Reducer<Text, Text, Text, IntWritable> {
	// Hydereabad [ {39,3} {54,13} {9,5) {39,6}........ ]
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context c)
			throws IOException, java.lang.InterruptedException {

		int totalRevenue = 0;

		Iterator<Text> valuesIter = values.iterator();

		/* For each store location */
		while (valuesIter.hasNext()) {
			/* product_price, no.of sales */
			String[] data = valuesIter.next().toString().split(","); // [ {39} {3} ]

			int price = Integer.parseInt(data[0]); // price = 39
			int sales = Integer.parseInt(data[1]); // sales = 3

			totalRevenue += (price * sales);
		}

		/* store location , totalRevenue */
		c.write(key, new IntWritable(totalRevenue));
	}
}
