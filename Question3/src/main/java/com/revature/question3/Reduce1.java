package com.revature.question3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	@Override
	public void reduce(Text text, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException{
		for (DoubleWritable num : value) {
			if (num.get() >= 0) {
				context.write(text, num);
			}
		}
	}
}