package com.tmj.cse;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] interMedKeys = value.toString().split(" ");
		for (String interMedKey : interMedKeys) {
			context.write(new Text(interMedKey), new IntWritable(1));
		}
	}
}
