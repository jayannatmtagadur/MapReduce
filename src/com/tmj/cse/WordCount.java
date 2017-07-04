package com.tmj.cse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length < 2) {
			System.out.println(" Please provide the proper input and output directory");
			System.out.println(" USAGE: hadoop jar <jar_name> <input_directory> <output_directory>");
			System.exit(0);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, " New Word Count Job");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setCombinerClass(WCReducer.class);   // Combiner class implementation
		job.setNumReduceTasks(3);
		job.setPartitionerClass(WCPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
