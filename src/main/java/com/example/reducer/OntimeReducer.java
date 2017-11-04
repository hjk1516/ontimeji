package com.example.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.example.datekey.DateKey;

public class OntimeReducer extends Reducer<DateKey, IntWritable, Text, Text> {
	
	
		Text outputkey = new Text();
		Text outvalue = new Text();
	
		
	public void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum=0;
		
		for (IntWritable value : values){
			sum += value.get();
		}
		
		outputkey = new Text("");		
		outvalue = new Text(key.getYear()+","+key.getUniquecarrier()+","+sum);
		context.write(outputkey, outvalue);	
	}
	
	
	
	


}
