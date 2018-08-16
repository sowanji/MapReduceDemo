package com.doit.chap03;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * @author Administrator
 *
 */

public class AggregateByKeyDriver extends Configured implements Tool {

	private static Logger THE_LOGGER = Logger.getLogger(AggregateByKeyDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(AggregateByKeyDriver.class);
		job.setJobName("AggregateByKeyDriver");
		
		job.setMapperClass(AggregateByKeyMapper.class);
		job.setReducerClass(AggregateByKeyReducer.class);
		job.setCombinerClass(AggregateByKeyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);
		THE_LOGGER.info("run(): status=" + status);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int num = 2;
		if (args.length != num) {
			THE_LOGGER.warn("use AggreageByKeyDriver <input> <output>");
			System.exit(1);
		}

		THE_LOGGER.info("inputDir=" + args[0]);
		THE_LOGGER.info("outputDir=" + args[1]);
		int returnStatus = ToolRunner.run(new AggregateByKeyDriver(), args);
		System.exit(returnStatus);
	}
}
