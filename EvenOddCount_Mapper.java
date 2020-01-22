package com.hadoop.learn.mr.evenodd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EvenOddCount_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//Sample data byteOffset is zero 0 34,35,66,100 ....
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String lineSplitValues[]=value.toString().split(",");
		//As we can write only Text and Writable datatypes to Hadoop
		Text even=new Text("EVEN");
		Text odd=new Text("ODD");
		
		for(String number: lineSplitValues) {
			int num=Integer.parseInt(number);
			//Check whether the number is EVEN or ODD
			if(num%2==0)
				context.write(even, new IntWritable(num));
			else
				context.write(odd, new IntWritable(num));
		}
	}

}
