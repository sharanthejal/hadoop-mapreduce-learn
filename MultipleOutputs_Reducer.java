package com.hadoop.learn.mr.multipleoutputs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//DJPX255251        [{ Arthur,HR,6397} { Arthur,HR,6597} {Arthur,HR,6797} ]
public class MultipleOutputs_Reducer extends Reducer<Text, Text, Text, Text> {

	MultipleOutputs<Text, Text> out = null;

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// Initialize MultipleOutputs object
		out = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		Iterator<Text> itr = values.iterator();
		int totalSalary = 0;
		String dept = "";
		String name = "";
		while (itr.hasNext()) {

			/* name,department,salary */
			String[] mapValueRecord = itr.next().toString().split(","); // [{ Arthur} {HR} {6397}]
			name = mapValueRecord[0]; // name = Arthur
			dept = mapValueRecord[1]; // dept = HR
			totalSalary += Integer.parseInt(mapValueRecord[2]); // totalSalary = 19791
		}
		/* output employee salary to department file */
			if (dept.equalsIgnoreCase("HR")) {
				out.write("HR", key, new Text(name + "," + totalSalary));
			} else if (dept.equalsIgnoreCase("Accounts")){
				out.write("Accounts", key, new Text(name + "," + totalSalary));
			}

	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		out.close();
	}
}
