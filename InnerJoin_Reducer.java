package com.hadoop.learn.mr.joins.inner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InnerJoin_Reducer extends Reducer<Text, Text, Text, Text> {

	// 1, [ {Emp,Jack} { Address,Paris} ]
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		ArrayList<String> empList = new ArrayList<String>();			// jack
		ArrayList<String> addressList = new ArrayList<String>();		//Paris

		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {

			String[] valueArr = itr.next().toString().split(",");
			if (valueArr[0].equalsIgnoreCase("Emp")) {
				empList.add(valueArr[1]);
			} else if (valueArr[0].equalsIgnoreCase("Address")) {
				addressList.add(valueArr[1]);
			}

		}

		if (!empList.isEmpty() && !addressList.isEmpty()) {
			for (String emp : empList) {
				for (String address : addressList) {
					context.write(new Text(key), new Text(emp + "," + address));
				}
			}
		}
	}
}
