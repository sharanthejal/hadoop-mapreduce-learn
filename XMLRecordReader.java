package com.hadoop.learn.mr.custominputandrecordreader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class XMLRecordReader extends RecordReader<LongWritable, Text> {

	private final String startTag = "<MOVIES>";
	private final String endTag = "</MOVIES>";

	private LineReader lineReader;

	private long curr_position = 0;
	private long startofFile;
	private long EndOfFile;

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fs1 = (FileSplit) split;
		Configuration con = context.getConfiguration();

		startofFile = fs1.getStart();

		EndOfFile = startofFile + fs1.getLength();

		Path file = fs1.getPath();

		FileSystem fs = file.getFileSystem(con);

		FSDataInputStream fsin = fs.open(fs1.getPath());
		fsin.seek(startofFile);

		lineReader = new LineReader(fsin, con);

		this.curr_position = startofFile;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		/* clear any previous values */
		key.set(curr_position);
		value.clear();

		Text line = new Text();
		boolean startFound = false;
		while (curr_position < EndOfFile) {
			long linelength = lineReader.readLine(line); // 8
			curr_position = curr_position + linelength;

			if (!startFound && line.toString().equalsIgnoreCase(this.startTag)) {
				/* start tag found and start reading further */
				startFound = true;

			} else if (startFound && line.toString().equalsIgnoreCase(this.endTag)) {
				/* end tag found, stop reading, remove last comma */
				String withoutComma = value.toString().substring(0, value.toString().length() - 1);
				value.set(withoutComma);
				return true;
			} else if (startFound) {
				/* read all data between start and end tag */
				String S1 = line.toString();
				/* remove xml tags from line */
				String content = S1.replaceAll("<[^>]+>", ""); // content = Titanic

				value.append(content.getBytes("utf-8"), 0, content.length());
				value.append(",".getBytes("utf-8"), 0, ",".length());
			}
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key; // returning key to framework whihc got calculated in above method
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value; // returning value to framework whihc got calculated in above method
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		/* return percentage of file read so far */
		return (curr_position - startofFile) / (float) (EndOfFile - startofFile);
		// 37 - 0 900 - 0 0.041
	}

	@Override
	public void close() throws IOException {
		/// close the linereader
		if (lineReader != null)

			lineReader.close();

	}

}
