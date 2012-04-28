package org.hackreduce.awesome;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class AwesomeWritable implements Writable {

	private LongWritable googleBooks;
	private LongWritable wikipedia;
	
	public AwesomeWritable(LongWritable googleBooks, LongWritable wikipedia) {
		this.googleBooks = googleBooks;
		this.wikipedia = wikipedia;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		googleBooks.readFields(in);
		wikipedia.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		googleBooks.write(out);
		wikipedia.write(out);
	}

	public LongWritable getGoogleBooks() {
		return googleBooks;
	}
	
	public LongWritable getWikipedia() {
		return wikipedia;
	}
	
	public void setGoogleBooks(LongWritable newValue) {
		googleBooks = newValue;
	}
	
	public void setWikipedia(LongWritable newValue) {
		wikipedia = newValue;
	}
}