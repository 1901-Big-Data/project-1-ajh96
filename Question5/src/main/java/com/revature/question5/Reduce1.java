package com.revature.question5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	public void reduce(Text text, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
		for (IntWritable num : value) {
			if (num.get() >= 0) {
				context.write(text, new IntWritable(num.get()));
			}
		}
	}
}