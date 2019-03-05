package com.revature.question3;

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
		line = line.replace("\"", "");
		String[] rows = line.split(",");
		String CountryName = rows[0];
		String IndicatorCode = rows[3];
		int year = 1960;
		switch(IndicatorCode) {
		case "SL.AGR.EMPL.MA.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Male Employment % in Agriculture: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Male Employment % in Agriculture: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.MPYR.MA.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Male Employers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Male Employers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.OWAC.MA.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Own-Account Male Workers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Own-Account Male Workers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.SELF.MA.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Self-Employed Male Workers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Self-Employed Male Workers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.WORK.MA.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Wage/Salaried Male Workers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Wage/Salaried Male Workers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.IND.EMPL.MA.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Male Employment % in Industry: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Male Employment % in Industry: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.SRV.EMPL.MA.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Male Employment % in Services: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Male Employment % in Services: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.TOTL.SP.MA.NE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Employment to Population Ratio, Male: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Employment to Population Ratio, Male: " + CountryName + ", " + year), new DoubleWritable(Data));
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
