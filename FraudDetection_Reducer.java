package com.hadoop.learn.mr.frauddetection;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FraudDetection_Reducer extends Reducer<Text, FraudDetection_Writable, Text, IntWritable> {

	//// GGYZ333519YS,[{Allison,13-01-2017,yes,15-01-2017},{Allison,26-01-2017,yes,16-02-2017}.....]
	ArrayList<String> customers = new ArrayList<String>();

	@Override
	protected void reduce(Text key, Iterable<FraudDetection_Writable> values,
			Reducer<Text, FraudDetection_Writable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		Iterator<FraudDetection_Writable> iterator = values.iterator();
		// HashMap<String, String> hm = new HashMap<String, String>();

		FraudDetection_Writable writable = null;
		int fraudPoints = 0;
		int ordersCount = 0;
		int returnsCount = 0;

		while (iterator.hasNext()) {

			ordersCount++;

			writable = iterator.next();
//			String customerName = writable.getCustomerName();
			String deliveredDate = writable.getdeliveredDate();
			boolean isReturned = writable.getReturned();
			String returnedDate = writable.getReturnedDate();

			if (isReturned) {

				// If isReturned is true, increase the returns count.
				returnsCount++;

				SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
				try {
					Date deliveredDateNew = sdf.parse(deliveredDate);
					Date returnedDateNew = sdf.parse(returnedDate);
					long diffInMillies = Math.abs(returnedDateNew.getTime() - deliveredDateNew.getTime());
					long diffDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);

					if (diffDays > 10L)
						fraudPoints++;

				} catch (ParseException e) {
					e.printStackTrace();
				}

			}

		}

		/* 10 fraud points to the customer whose return rate is more than 50% */
		double returnRate = (returnsCount / (ordersCount * 1.0)) * 100;
		if (returnRate >= 50)
			fraudPoints += 10;
		customers.add(key.toString() + "," + writable.getCustomerName() + "," + fraudPoints);
	}

	@Override
	protected void cleanup(Context c) throws IOException, java.lang.InterruptedException {
		/* sort customers based on fraudpoints */
		Collections.sort(customers, new Comparator<String>() {
			public int compare(String s1, String s2) {
				int fp1 = Integer.parseInt(s1.split(",")[2]);
				int fp2 = Integer.parseInt(s2.split(",")[2]);

				return -(fp1 - fp2); /* For desscending order */
			}
		});
		for (String f : customers) {
			String[] words = f.split(",");
			c.write(new Text(words[0] + "," + words[1]), new IntWritable(Integer.parseInt(words[2])));
		} // custID // custname // fraud points
	}
	
	/*public static void main(String[] args) {
		int fraudPoints=0;
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		try {
			Date deliveredDateNew = sdf.parse("08-01-2017");
			Date returnedDateNew = sdf.parse("20-01-2017");
			long diffInMillies = Math.abs(returnedDateNew.getTime() - deliveredDateNew.getTime());
			System.out.println(diffInMillies);
			long diffDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);

			System.out.println("DiffDays "+diffDays );
			if (diffDays > 10L)
				fraudPoints++;

			System.out.println(fraudPoints);
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}*/
}
