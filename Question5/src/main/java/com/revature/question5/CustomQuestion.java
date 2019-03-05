package com.revature.question5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomQuestion extends Configured implements Tool{
	//My custom question looks into how much money a county's government invests in
	//schooling per GDP capita: Primary per student, Secondary per student
	//and Total investment 
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: CustomQuestionDriver <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(CustomQuestion.class);
		job.setJobName("Custom Question Driver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new CustomQuestion(), args);
		System.exit(exitCode);
	}
}
