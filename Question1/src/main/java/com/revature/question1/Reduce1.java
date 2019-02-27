package com.revature.question1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	public void reduce(Text text, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
		int sum = 0;
		int count = 0;
		int average = 0;
		int max = 0;
		int year = 1960;
		int maxYear = 2016;
		for (IntWritable num : value) {
			if (num.get() >= 0) {
				sum += num.get();
				count++;
			}
			if (num.get() >= max) {
				max = num.get();
				maxYear = year;
			}
			year++;
		}
		average = sum / count;
		if (average < 30) {
			context.write(text, new IntWritable(average));
		}
		if (max < 30) {
			context.write(new Text("Year of Maximum: " + maxYear),  new IntWritable(max));
		}
	}
}
