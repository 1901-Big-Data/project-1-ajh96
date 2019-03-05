package com.revature.question1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	@Override
	public void reduce(Text text, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException{
		double sum = 0;
		int count = 0;
		double average = 0;
		double max = 0;
		int year = 1960;
		int maxYear = 2016;
		for (DoubleWritable num : value) {
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
		if (count == 0) {
			count = 1;
		}
		average = sum / count;
		if (average < 30) {
			context.write(text, new DoubleWritable(average));
			if (max >= 30) {
				context.write(new Text("Year of Maximum: " + maxYear),  new DoubleWritable(max));
			}
		}
	}
}
