package com.revature.question1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException{
		String line = text.toString();
		line = line.replace(", ", " ");
		line = line.replace("\"", "");
		String[] rows = line.split(",");
		String CountryName = rows[0];
		String IndicatorCode = rows[3];
		switch(IndicatorCode) {
		case "SE.PRM.CUAT.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0) {
						context.write(new Text("Primary School: " + CountryName), new DoubleWritable(-1));
					} else {
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Primary School: " + CountryName), new DoubleWritable(Data));
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SE.TER.CUAT.BA.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0) {
						context.write(new Text("Bachelor's: " + CountryName), new DoubleWritable(-1));
					} else {
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Bachelor's: " + CountryName), new DoubleWritable(Data));
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		default:
			break;
		}
	}
}
