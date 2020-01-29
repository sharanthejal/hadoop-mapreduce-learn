package com.hadoop.learn.mr.joins.outer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OuterJoinDept_Mapper extends Mapper<LongWritable, Text, Text, Text>
{
	//  10,Inventory,HYDERABAD 
		
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
	{
		
		String line = value.toString().trim();      //  line =   10,Inventory,HYDERABAD 
		
		String[] valueArray = line.split(",");       // valueArray = [{10} {Inventory} {HYDERABAD}]
		
		context.write(new Text(valueArray[0]), new Text("department,"+valueArray[1]+" "+valueArray[2]));
	}
}
