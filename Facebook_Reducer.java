package com.hadoop.learn.mr.fb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Facebook_Reducer extends Reducer<Text, Text, Text, Text> {

	// Input will be Key= Health, Value= [Hyderabad+271+14,Hyderabad+271+14,.....]
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> itr = values.iterator();
		HashMap<String, String> hm = new HashMap<String, String>();

		while (itr.hasNext()) {

			String val = itr.next().toString();
			String[] arr = val.split(",");

			String location = arr[0];

			Double average = (Double.parseDouble(arr[2]) / Double.parseDouble(arr[1])) * 100;

			if (hm.get(location) != null && hm.containsKey(location)) {
				String[] valu = hm.get(location).split(",");
				Double average1 = Double.parseDouble(valu[0]) + average;
				int counter = Integer.parseInt(valu[1]);
				hm.put(location, average1 + "," + counter);
			} else {
				hm.put(location, average + ",1");
			}

		}

		for (Map.Entry<String, String> e : hm.entrySet())        // e = Mumbai  value  65.07,3
		{
		    String[] V1 = e.getValue().split(",");        // V1  [{65.07} {3}]
		    Double avgSccRate = Double.parseDouble(V1[0])/Integer.parseInt(V1[1]);
		    context.write(key, new Text(e.getKey() + "," + avgSccRate));
		}
	}
}
