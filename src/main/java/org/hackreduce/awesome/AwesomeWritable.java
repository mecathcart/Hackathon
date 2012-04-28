package org.hackreduce.awesome;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AwesomeWritable implements Writable {

	private long googleBooks;
	private long wikipedia;
	
	public AwesomeWritable() {
		
	}
	
	public AwesomeWritable(LongWritable googleBooks, LongWritable wikipedia) {
		this.googleBooks = googleBooks.get();
		this.wikipedia = wikipedia.get();
	}
	
	public AwesomeWritable(String[] splittedValues) {
		this(new LongWritable(Long.parseLong(splittedValues[0])), new LongWritable(Long.parseLong(splittedValues[1])));
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		googleBooks = in.readLong();
		wikipedia = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(googleBooks);
		out.writeLong(wikipedia);
	}

	public LongWritable getGoogleBooks() {
		return new LongWritable(googleBooks);
	}
	
	public LongWritable getWikipedia() {
		return new LongWritable(wikipedia);
	}
	
	public void setGoogleBooks(LongWritable newValue) {
		
		googleBooks = newValue.get();
	}
	
	public void setWikipedia(LongWritable newValue) {
		wikipedia = newValue.get();
	}
	
	public void setGoogleBooks(long newValue) {
		googleBooks = newValue;
	}
	
	public void setWikipedia(long newValue) {
		wikipedia = newValue;
	}
	
	public String toString() {
		return googleBooks + "\t" + wikipedia;
	}
	
	public void addMoreAwesomeStuff(AwesomeWritable aw) {
		this.googleBooks += aw.googleBooks;
		this.wikipedia += aw.wikipedia; 
	}
}