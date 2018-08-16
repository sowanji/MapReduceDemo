package com.doit.chap03;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Administrator
 *
 */

public class AggregateByKeyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text k2 = new Text();
	private IntWritable v2 = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valueAsString = value.toString().trim();
		String[] tokens = valueAsString.split(",");
		String url = tokens[0];
		int frequency = Integer.parseInt(tokens[1]);
		k2.set(url);
		v2.set(frequency);
		context.write(k2, v2);
	}

}
