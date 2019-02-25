package com.revature.question4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException{
		String line = text.toString();
		//add implementation
	}
}
