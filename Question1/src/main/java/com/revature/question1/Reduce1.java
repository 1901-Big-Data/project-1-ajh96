package com.revature.question1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	public void reduce(Text text, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
		//add implementation
	}
}
