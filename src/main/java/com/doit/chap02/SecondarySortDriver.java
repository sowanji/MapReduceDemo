package com.doit.chap02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class SecondarySortDriver extends Configured implements Tool{
	private static Logger theLogger = Logger.getLogger(SecondarySortDriver.class);

	public static void main(String[] args) throws Exception {
		int num = 2;
		if (args.length != num) {
			theLogger.warn("SecondarySortDriver <input-dir> <output dir>");
			throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output dir>");
		}

		int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
		theLogger.info("returnStatus=" + returnStatus);
		System.exit(returnStatus);

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SecondarySortDriver.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(NaturalValue.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
	    job.setSortComparatorClass(CompositeKeyComparator.class);
		//job.setCombinerKeyGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);
		theLogger.info("run(): status=" + status);
		return status ? 0 : 1;
	}

}
