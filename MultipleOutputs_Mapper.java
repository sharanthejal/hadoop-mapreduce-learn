package com.hadoop.learn.mr.multipleoutputs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Sample Input -- DJPX255251,Arthur,HR,6397,2016 
public class MultipleOutputs_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String values[]= value.toString().split(",");
		
		String empId= values[0];
		String mapperValue= values[1]+","+values[2]+","+values[3];
		
		context.write(new Text(empId), new Text(mapperValue));  //DJPX255251 -> Arthur,HR,6397
	}
}
