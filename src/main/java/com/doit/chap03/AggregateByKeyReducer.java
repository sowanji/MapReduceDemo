package com.doit.chap03;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Administrator
 *
 */

public class AggregateByKeyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable v4 = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		v4.set(sum);
		context.write(key, v4);
	}

}
