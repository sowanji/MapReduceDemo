package com.doit.chap02;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable,Text,CompositeKey,NaturalValue>{
	
	private final CompositeKey reducerKey = new CompositeKey();
	private final NaturalValue reducerValue = new NaturalValue();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CompositeKey, NaturalValue>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = StringUtils.split(value.toString().trim(), ",");
		Date date = DateUtil.getDate(tokens[1]);
		if(date == null) {
			return;
		}
		
		long timestamp = date.getTime();
		reducerKey.setStockSymbol(tokens[0]);
		reducerKey.setTimestamp(timestamp);
		reducerValue.setTimestamp(timestamp);
		reducerValue.setPrice(Double.parseDouble(tokens[2]));
		
		context.write(reducerKey, reducerValue);
	}
	
	

}
