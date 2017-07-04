package com.tmj.cse;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numreducer) {
		// TODO Auto-generated method stub
		int keylen = key.toString().length();
		if (keylen < 3) {
			return 0;
		} else if (keylen >= 3 && keylen <= 5) {
			return 1;

		} else {
			return 2;
		}
	}

}
