package com.doit.chap02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{
	
	private String stockSymbol;
	private long timestamp;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.stockSymbol);
		out.writeLong(this.timestamp);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stockSymbol = in.readUTF();
		this.timestamp = in.readLong();
	}

	@Override
	public int compareTo(CompositeKey other) {
		int result = this.stockSymbol.compareTo(other.stockSymbol);
		if(result != 0) {
			return result;
		}else if (this.timestamp != other.timestamp) {
			return this.timestamp < other.timestamp ? -1 : 1;
		}else {
			return 0;
		}
	}

	public String getStockSymbol() {
		return stockSymbol;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setStockSymbol(String stockSymbol) {
		this.stockSymbol = stockSymbol;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	

}
