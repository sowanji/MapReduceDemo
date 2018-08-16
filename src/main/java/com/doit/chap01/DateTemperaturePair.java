package com.doit.chap01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DateTemperaturePair implements WritableComparable<DateTemperaturePair>{
	
	private String yearMonth;
	private String day;
	private Integer temperature;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(yearMonth);
		out.writeUTF(day);
		out.writeInt(temperature);			
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.yearMonth = in.readUTF();
		this.day = in.readUTF();
		this.temperature = in.readInt();
	}

	@Override
	public int compareTo(DateTemperaturePair pair) {
		int compareValue = this.yearMonth.compareTo(pair.yearMonth);
		if(compareValue == 0) {
			compareValue = this.temperature.compareTo(pair.temperature);
		}
		return -1*compareValue;
	}

	public String getYearMonth() {
		return yearMonth;
	}

	public String getDay() {
		return day;
	}

	public Integer getTemperature() {
		return temperature;
	}

	public void setYearMonth(String yearMonth) {
		this.yearMonth = yearMonth;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public void setTemperature(Integer temperature) {
		this.temperature = temperature;
	}

	@Override
	public String toString() {
		return "DateTemperaturePair [yearMonth=" + yearMonth + ", day=" + day + ", temperature=" + temperature + "]";
	}
}
