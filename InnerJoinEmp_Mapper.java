package com.hadoop.learn.mr.joins.inner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//1, Jack -- This is reduce join, so all the functionality is written in Reducer code
public class InnerJoinEmp_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] values = value.toString().trim().split(",");
		context.write(new Text(values[0].trim()), new Text("Emp,"+values[1].trim()));  // Key =1, Value = Jack

	}
}
