package com.example.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.datekey.DateKey;
import com.example.parser.OntimeParser;

public class OntimeMapper extends Mapper<LongWritable, Text, DateKey, IntWritable> {

	private DateKey outputKey = new DateKey();
	private IntWritable outValue = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		OntimeParser parser = new OntimeParser(value);

		outputKey.setYear(parser.getYear());
		outputKey.setUniquecarrier(parser.getUniquecarrier());
		outValue.set(1);
		context.write(outputKey, outValue);
	}

}
