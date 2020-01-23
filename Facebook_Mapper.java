package com.hadoop.learn.mr.fb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//CFVV945456VL,2010-01-03 23:40:21,Hyderabad,Health,271,14,40-45
public class Facebook_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		
		String values[]= value.toString().split(",");
		Text adType= new Text(values[3]);
		Text adInfo= new Text(values[2]+new Text(",")+values[4]+new Text(",")+values[5]);
		
		/*The data from the Mapper will be
		 * Key=Health, Value= Hyderabad+271+14
		 */
		context.write(adType,adInfo);
	}

}
