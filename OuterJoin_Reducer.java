package com.hadoop.learn.mr.joins.outer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OuterJoin_Reducer extends Reducer<Text, Text, Text, Text> {

	/*
	 * 10, [{employee,1281 Shawn Architect 7890 1481},{employee,1281 Shawn Architect
	 * 7890 1481},.. {department,Inventory HYDERABAD}] 10 is department id There can
	 * be many employees with the same department id but department will be one.
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<String> empList = new ArrayList<String>();
		String department = "";
		Iterator<Text> itr = values.iterator();

		while (itr.hasNext()) {

			String[] strArray = itr.next().toString().split(",");

			if (strArray[0].equalsIgnoreCase("employee")) {
				empList.add(strArray[1]);
			} else if (strArray[0].equalsIgnoreCase("department")) {
				department = strArray[1];
			}
		}

		// Inner Join
		if (!empList.isEmpty() && !department.isEmpty()) {
			for (String employee : empList) {
				context.write(key, new Text(employee + "," + department));
			}
		}

		// Left Outer Join
		if (!empList.isEmpty() && department.isEmpty()) {
			for (String employee : empList) {
				context.write(key, new Text(employee + "," + "NULL NULL"));
			}
		}

		// Left Outer Join
		if (empList.isEmpty() && !department.isEmpty()) {
			context.write(key, new Text("NULL NULL NULL NULL NULL" + "," + department));
		}

	}
}
