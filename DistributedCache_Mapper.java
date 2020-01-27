package com.hadoop.learn.mr.distributedcache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

@SuppressWarnings("deprecation")
public class DistributedCache_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private HashMap<String, Double> desg_map = new HashMap<String, Double>(); // [ [ {MGR: 2} {DLP:5} {HR:6} ] ]

	@SuppressWarnings({ "unused", "deprecation" })
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		/* read data from distributed cache */
		BufferedReader br = null;
		Path[] localCacheFilesPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		String record = "";
		for (Path path : localCacheFilesPath) {

			if (path.getName().equalsIgnoreCase("designation.txt")) {
				br = new BufferedReader(new FileReader(path.toString()));
				record = br.readLine(); // MGR: 2
				while (record != null) {
					String data[] = record.split(",");
					/* designation_code, increment_multiplier */
					desg_map.put(data[0].trim(), Double.parseDouble(data[1].trim()));
					record = br.readLine();
				}

			}
		}

	}

	// 607949MR,Allison,Developer,1414,4.4
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		/* Split csv string */
		String[] words = line.split(","); // [{607949MR} {Allison} {Developer} {1414} {4.4} ]

		String designation = words[2]; // designation = Developer

		double n = 1;
		if (designation.toString().equalsIgnoreCase("manager")) {
			n = desg_map.get("MGR"); // n= 2
		} else if (designation.toString().equalsIgnoreCase("developer")) {
			n = desg_map.get("DLP"); // n = 5
		} else if (designation.toString().equalsIgnoreCase("hr")) {
			n = desg_map.get("HR"); // n = 6
		} else {
			System.out.println("Invalid designation");
		}

		int currentSalary = Integer.parseInt(words[3].trim());
		//Not satisfied with the below increment logic as provided.
		double increment = (n / 100) * currentSalary;

		context.write(new Text(designation), new DoubleWritable(increment));
	}
}
