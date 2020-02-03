package com.hadoop.learn.mr.counters;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

enum LOCATION {
	TOTAL, BANGALORE, CHENNAI, HYDERABAD
}

public class CounterMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text storeLocation = new Text();
	private Text data = new Text();

	@Override
	// 1194208577,sofa,39,3,Hyderabad
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, java.lang.InterruptedException {

		/* total records processed by all mappers */
		context.getCounter(LOCATION.TOTAL).increment(1);

		String line = value.toString(); // line = 1194208577,sofa,39,3,Hyderabad

		String[] words = line.split(","); // [{1194208577} {sofa} {39} {3} {Hyderabad}]

		/* location counters */
		if (words[4].equalsIgnoreCase("bangalore")) {
			context.getCounter(LOCATION.BANGALORE).increment(1);
		} else if (words[4].equalsIgnoreCase("chennai")) {
			context.getCounter(LOCATION.CHENNAI).increment(1);
		} else if (words[4].equalsIgnoreCase("hyderabad")) {
			context.getCounter(LOCATION.HYDERABAD).increment(1);
		} else
			throw new RuntimeException("No such city");

		/* sale counters - dynamic */
		int salesCount = Integer.parseInt(words[3]);
		if (salesCount < 10)

			context.getCounter("SALES", "LOW_SALES").increment(1);

		int price = Integer.parseInt(words[2]);
		if ((salesCount * price) > 500)

			context.getCounter("SALES", "HIGH_REVENUE").increment(1);

		storeLocation.set(words[4]); // storeLocation = Hyderabad
		/* product_price, no.of sales */
		data.set(words[2] + "," + words[3]); // data = 39,3

		context.write(storeLocation, data);
	}
}
