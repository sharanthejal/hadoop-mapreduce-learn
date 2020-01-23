package com.hadoop.learn.mr.customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CustomTextWritable implements WritableComparable<CustomTextWritable> {

	String word;

	public CustomTextWritable() {

	}

	public CustomTextWritable(String word) {
		// this.word = word;
		setWord(word);
	}

	// Setters and Getters
	public String getWord() {
		return this.word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	// Override methods
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, this.word);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word = WritableUtils.readString(in);

	}

	@Override
	public int compareTo(CustomTextWritable arg0) {
		return this.word.compareTo(arg0.getWord());
	}

}
