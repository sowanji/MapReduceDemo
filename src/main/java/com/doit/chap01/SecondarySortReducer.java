package com.doit.chap01;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {

	private final Text yearMonth = new Text();
	private final Text temperatures = new Text();

	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values,
			Reducer<DateTemperaturePair, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		for (Text value : values) {
			builder.append(value.toString()).append(";");
		}
		yearMonth.set(key.getYearMonth().toString());
		temperatures.set(builder.toString());
		context.write(yearMonth, temperatures);
	}

}
