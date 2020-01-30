package com.hadoop.learn.mr.distributedcache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Currently using the below class we have to read the data from the local fs
 */
public class DistributedCache_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private HashMap<String, Double> desg_map = new HashMap<String, Double>(); // [ [ {MGR: 2} {DLP:5} {HR:6} ] ]

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		/* read data from distributed cache */
		BufferedReader br = null;
		/* Below DistributedCache Class is deprecated and moved to Job class */
		// Path[] localCacheFilesPath =
		// DistributedCache.getLocalCacheFiles(context.getConfiguration());
		URI[] uris = context.getCacheFiles();

		Path[] localCacheFilesPath = new Path[5];
		int count = 0;
		String record = "";
		for (URI uri : uris) {
			localCacheFilesPath[count] = new Path(uri);
			if (localCacheFilesPath[count].getName().equalsIgnoreCase("designation.txt")) {
				br = new BufferedReader(new FileReader(uri.getPath()));
				record = br.readLine(); // MGR: 2
				while (record != null) {
					String data[] = record.split(",");
					/* designation_code, increment_multiplier */
					desg_map.put(data[0].trim(), Double.parseDouble(data[1].trim()));
					record = br.readLine();
				}

			}

			count++;
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
		// Not satisfied with the below increment logic as provided.
		double increment = (n / 100) * currentSalary;

		context.write(new Text(designation), new DoubleWritable(increment));
	}

	public static void main(String[] args) throws URISyntaxException {
		URI uri = new URI("hdfs://localhost:8020/tmp/sharan/designation.txt");
		System.out.println(uri.getHost() + uri.getPort() + uri.getPath());
		System.out.println(uri.getPath());
	}
}
