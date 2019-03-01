package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.question1.*;

public class Question1Test {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setup() {
		Map1 Mapper = new Map1();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(Mapper);
		
		Reduce1 Reducer = new Reduce1();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(Reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(Mapper);
		mapReduceDriver.setReducer(Reducer);
	}
	
	@Test
	public void testMapperFullData() {
		String input = "America,USA,Female Graduation Rates,SE.TER.CUAT.BA.FE.ZS,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		for (int x = 4; x < 61; x++) {
			mapDriver.withOutput(new Text("Bachelor's: America"), new DoubleWritable(0));
		}
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperEmptyData() {
		String input = "America,USA,Female Graduation Rates,SE.PRM.CUAT.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,50";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		for (int x = 4; x < 61; x++) {
			if (x != 60) {
				mapDriver.withOutput(new Text("Primary School: America"), new DoubleWritable(-1));
			} else {
				mapDriver.withOutput(new Text("Primary School: America"), new DoubleWritable(50));
			}
			
		}
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperIncorrectSeriesCode() {
		String input = "America,USA,Female Graduation Rates,SL.TLF.CACT.FE.NE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,50";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		//check if an input that doesn't match the series-code isn't mapped
		mapDriver.runTest();
	}
	
	@Test
	public void testAverageSum() {
		List<DoubleWritable> list = new ArrayList<DoubleWritable>();
		list.add(new DoubleWritable(5));
		list.add(new DoubleWritable(10));
		list.add(new DoubleWritable(15));
		list.add(new DoubleWritable(20));
		reduceDriver.withInput(new Text("Primary School: America"), list);
		reduceDriver.withOutput(new Text("Primary School: America"), new DoubleWritable(12.5));
		reduceDriver.withOutput(new Text("Year of Maximum: 1963"), new DoubleWritable(20));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String input = "America,USA,Female Graduation Rates,SE.TER.CUAT.BA.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,12,,,,3,15,,,,,,,,,,,29,,,0,,,8,,,,,,,,,,4";
		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
		mapReduceDriver.withOutput(new Text("Bachelor's: America"), new DoubleWritable(10.142857142857142));
		mapReduceDriver.withOutput(new Text("Year of Maximum: 2000"), new DoubleWritable(29));
		mapReduceDriver.runTest();
	}
}
