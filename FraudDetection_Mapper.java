package com.hadoop.learn.mr.frauddetection;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//GGYZ333519YS,Allison,08-01-2017,10-01-2017,Delhivery,13-01-2017,yes,15-01-2017,Damaged Item
public class FraudDetection_Mapper extends Mapper<LongWritable, Text, Text, FraudDetection_Writable> {

	FraudDetection_Writable writable = new FraudDetection_Writable();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, FraudDetection_Writable>.Context context)
			throws IOException, InterruptedException {

		String[] values = value.toString().split(",");
		String id = values[0];

		String name = values[1];
		String deliveredDate = values[5];
		String returned = values[6];
		String returnedDate = values[7];

		writable.set(name, deliveredDate, returned, returnedDate);

		context.write(new Text(id), writable);

	}

}
