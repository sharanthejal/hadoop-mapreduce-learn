package com.hadoop.learn.mr.joins.outer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OuterJoinEmp_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	// 1281,Shawn,Architect,7890,1481,10

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().trim(); // convert incoming record to string

		String[] employeeData = line.split(","); // [{ 1281} {Shawn} {Architect} {7890} {1481} {10} ]

		context.write(new Text(employeeData[5]), new Text("Employee," + employeeData[0] + " " + employeeData[1] + " "
				+ employeeData[2] + " " + employeeData[3] + " " + employeeData[4]));

	}
}
