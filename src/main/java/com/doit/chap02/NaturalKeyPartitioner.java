package com.doit.chap02;

import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey,NaturalValue>{

	@Override
	public int getPartition(CompositeKey key, NaturalValue value, int numPartitions) {
		return Math.abs(key.getStockSymbol().hashCode() % numPartitions);
	}

}
