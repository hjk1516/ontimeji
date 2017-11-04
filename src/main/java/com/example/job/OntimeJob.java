package com.example.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.example.datekey.DateKey;
import com.example.mapper.OntimeMapper;
import com.example.reducer.OntimeReducer;

public class OntimeJob {
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "OntimeJob");
		Path outputdir = new Path("/user/java/dataexpo/out");
		
//		FileInputFormat.addInputPath(job, new Path("/user/java/dataexpo/1987_nohead.csv"));
		FileInputFormat.addInputPath(job, new Path("/user/java/dataexpo"));
		FileOutputFormat.setOutputPath(job, outputdir);
		
		job.setJarByClass(OntimeJob.class);
		job.setMapperClass(OntimeMapper.class);
		job.setReducerClass(OntimeReducer.class);
		
		
//		job.setPartitionerClass();
		
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputdir, true);
		
		job.waitForCompletion(true);
	}

}
