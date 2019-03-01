package com.revature.question5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException{
		String line = text.toString();
		line = line.replace(", ", " ");
		String[] rows = line.split(",");
		String CountryName = rows[0];
		String IndicatorCode = rows[3];
		int year = 1960;
		switch(IndicatorCode) {
		case "SE.XPD.PRIM.PC.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Government Expediture (% of GDP per Capita) per Student: Primary School: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Government Expediture (% of GDP per Capita) per Student: Primary School: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SE.XPD.SECO.PC.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Government Expediture (% of GDP per Capita) per Student: Secondary School: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Government Expediture (% of GDP per Capita) per Student: Secondary School: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SE.XPD.TOTL.GD.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Government Expediture (% of GDP per Capita) on Education in General: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Government Expediture (% of GDP per Capita) on Education in General: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
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
