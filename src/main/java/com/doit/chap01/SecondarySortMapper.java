package com.doit.chap01;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {

	private final DateTemperaturePair pari = new DateTemperaturePair();
	private final Text text = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, DateTemperaturePair, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");
		String yearMonth = tokens[0] +"-"+ tokens[1];
		String day = tokens[2];
		String temperature = tokens[3];
		pari.setYearMonth(yearMonth);
		pari.setDay(day);
		pari.setTemperature(Integer.parseInt(temperature));
		text.set(temperature);
		context.write(pari, text);
	}

}
