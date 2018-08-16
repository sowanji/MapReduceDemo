package com.doit.chap02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NaturalValue implements WritableComparable<NaturalValue>{
	
	private long timestamp;
	private double price;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeDouble(price);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readLong();
		this.price = in.readDouble();
		
	}

	@Override
	public int compareTo(NaturalValue other) {
		if(this.timestamp < other.timestamp) {
			return -1;
		}else if(this.timestamp > other.timestamp) {
			return 1;
		}else {
			return 0;
		}
	}

	public long getTimestamp() {
		return timestamp;
	}

	public double getPrice() {
		return price;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	
}
