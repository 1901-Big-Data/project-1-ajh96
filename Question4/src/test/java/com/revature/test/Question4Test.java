package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.question4.*;

public class Question4Test {
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
		String input = "France,FRN,Female enrollment in agriculture, rep. in %,SL.AGR.EMPL.FE.ZS,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		int year = 2000;
		for (int x = 44; x < 61; x++) {
			mapDriver.withOutput(new Text("Female Employment % in Agriculture: France, " + year), new DoubleWritable(0));
			year++;
		}
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperEmptyData() {
		String input = "France,FRN,% of Female Employers,SL.EMP.MPYR.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,50";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		int year = 2000;
		for (int x = 44; x < 61; x++) {
			if (x != 60) {
				mapDriver.withOutput(new Text("% of Female Employers: France, " + year), new DoubleWritable(-1));
			} else {
				mapDriver.withOutput(new Text("% of Female Employers: France, " + year), new DoubleWritable(50));
			}
			year++;
		}
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperIncorrectSeriesCode() {
		String input = "Germany,GMY,Doesn't really care what this says so yeah,SL.TLF.CACT.FE.NE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,50";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		//check if an input that doesn't match the series-code isn't mapped
		mapDriver.runTest();
	}
	
	@Test
	public void testBasicReducer() {
		List<DoubleWritable> list = new ArrayList<DoubleWritable>();
		list.add(new DoubleWritable(-1));
		list.add(new DoubleWritable(5));
		list.add(new DoubleWritable(10));
		list.add(new DoubleWritable(15));
		list.add(new DoubleWritable(-1));
		list.add(new DoubleWritable(20));
		reduceDriver.withInput(new Text("This is just some test input text"), list);
		reduceDriver.withOutput(new Text("This is just some test input text"), new DoubleWritable(5));
		reduceDriver.withOutput(new Text("This is just some test input text"), new DoubleWritable(10));
		reduceDriver.withOutput(new Text("This is just some test input text"), new DoubleWritable(15));
		reduceDriver.withOutput(new Text("This is just some test input text"), new DoubleWritable(20));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String input = "France,FRN,% of Females in Inustrial Industry,SL.IND.EMPL.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,12,,,,3,15,,,,,,,,,,,31,,,0,,,8,,,,,,,,,,4";
		String input2 = "North Korea,NKO,% of Females in Service Industry,SL.SRV.EMPL.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,12,,,,3,15,,,,,,,,,,,31,,,0,,,8,,,,,,,,,,4";
		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
		mapReduceDriver.withInput(new LongWritable(-1), new Text(input2));
		mapReduceDriver.withOutput(new Text("Female Employment % in Industry: France, 2000"), new DoubleWritable(31));
		mapReduceDriver.withOutput(new Text("Female Employment % in Industry: France, 2003"), new DoubleWritable(0));
		mapReduceDriver.withOutput(new Text("Female Employment % in Industry: France, 2006"), new DoubleWritable(8));
		mapReduceDriver.withOutput(new Text("Female Employment % in Industry: France, 2016"), new DoubleWritable(4));
		mapReduceDriver.withOutput(new Text("Female Employment % in Services: North Korea, 2000"), new DoubleWritable(31));
		mapReduceDriver.withOutput(new Text("Female Employment % in Services: North Korea, 2003"), new DoubleWritable(0));
		mapReduceDriver.withOutput(new Text("Female Employment % in Services: North Korea, 2006"), new DoubleWritable(8));
		mapReduceDriver.withOutput(new Text("Female Employment % in Services: North Korea, 2016"), new DoubleWritable(4));
		mapReduceDriver.runTest();
	}
}
